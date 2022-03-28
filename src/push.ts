import type { LogContext } from "@rocicorp/logger";
import { nanoid } from "nanoid";
import type { MutatorDefs } from "replicache";
import { transact } from "./pg.js";
import {
  createDatabase,
  getCookie,
  getLastMutationID,
  setCookie,
  setLastMutationID,
} from "./data.js";
import { ReplicacheTransaction } from "./replicache-transaction.js";
import { z } from "zod";
import { jsonSchema } from "./json.js";

// TODO: Either generate schema from mutator types, or vice versa, to tighten this.
// See notes in bug: https://github.com/rocicorp/replidraw/issues/47
const mutationSchema = z.object({
  id: z.number(),
  name: z.string(),
  args: jsonSchema,
});

const pushRequestSchema = z.object({
  clientID: z.string(),
  mutations: z.array(mutationSchema),
});

export type Request = {
  spaceID: string;
  body: string;
};

export async function push(
  lc: LogContext,
  req: Request,
  mutators: MutatorDefs
) {
  lc.addContext("req", nanoid()).addContext("spaceID", "spaceID");
  lc.debug?.("Processing push", req.body);

  try {
    const push = pushRequestSchema.parse(req.body);
    lc.addContext("clientID", push.clientID);
    lc.debug?.("Got clientID");

    const t0 = Date.now();
    await transact(lc, async (executor) => {
      await createDatabase(lc, executor);

      const prevVersion = (await getCookie(executor, req.spaceID)) ?? 0;
      const nextVersion = prevVersion + 1;
      let lastMutationID =
        (await getLastMutationID(executor, push.clientID)) ?? 0;

      lc.debug?.("prevVersion: ", prevVersion);
      lc.debug?.("lastMutationID:", lastMutationID);

      const tx = new ReplicacheTransaction(
        executor,
        req.spaceID,
        push.clientID,
        nextVersion
      );

      for (let i = 0; i < push.mutations.length; i++) {
        const mutation = push.mutations[i];
        const expectedMutationID = lastMutationID + 1;

        if (mutation.id < expectedMutationID) {
          lc.debug?.(
            `Mutation ${mutation.id} has already been processed - skipping`
          );
          continue;
        }
        if (mutation.id > expectedMutationID) {
          lc.info?.(`Mutation ${mutation.id} is from the future - aborting`);
          break;
        }

        lc.debug?.("Processing mutation:", JSON.stringify(mutation, null, ""));

        const t1 = Date.now();
        const mutator = (mutators as any)[mutation.name];
        if (!mutator) {
          lc.error?.(`Unknown mutator: ${mutation.name} - skipping`);
        }

        try {
          await mutator(tx, mutation.args);
        } catch (e) {
          lc.error?.(
            `Error executing mutator: ${JSON.stringify(mutator)}: ${e}`
          );
        }

        lastMutationID = expectedMutationID;
        lc.debug?.("Processed mutation in", Date.now() - t1);
      }

      await Promise.all([
        setLastMutationID(executor, push.clientID, lastMutationID),
        setCookie(executor, req.spaceID, nextVersion),
        tx.flush(),
      ]);
    });

    lc.debug?.("Processed all mutations in", Date.now() - t0);
  } catch (e) {
    lc.error?.(e);
    throw e;
  }
}
