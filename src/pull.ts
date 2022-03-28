import { z } from "zod";
import { nanoid } from "nanoid";
import type { PullResponse } from "replicache";
import type { LogContext } from "@rocicorp/logger";
import { transact } from "./pg.js";
import {
  createDatabase,
  getChangedEntries,
  getCookie,
  getLastMutationID,
} from "./data.js";

const pullRequest = z.object({
  clientID: z.string(),
  cookie: z.union([z.number(), z.null()]),
});

export type Request = {
  spaceID: string;
  body: string;
};

export async function pull(lc: LogContext, req: Request): Promise<string> {
  lc = lc.addContext("req", nanoid()).addContext("spaceID", req.spaceID);
  lc.debug?.(`Processing pull: ${req}`);
  try {
    const { spaceID } = req;
    const pull = pullRequest.parse(req.body);
    const requestCookie = pull.cookie;

    lc = lc.addContext("clientID", pull.clientID);
    lc.debug?.("Got clientID");

    const t0 = Date.now();

    const [entries, lastMutationID, responseCookie] = await transact(
      lc,
      async (executor) => {
        await createDatabase(lc, executor);

        return Promise.all([
          getChangedEntries(executor, spaceID, requestCookie ?? 0),
          getLastMutationID(executor, pull.clientID),
          getCookie(executor, spaceID),
        ]);
      }
    );

    lc.debug?.(
      `lastMutationID: ${lastMutationID}, responseCookie: ${responseCookie}`
    );
    lc.debug?.("Read all objects in", Date.now() - t0);

    const resp: PullResponse = {
      lastMutationID: lastMutationID ?? 0,
      cookie: responseCookie ?? 0,
      patch: [],
    };

    for (const [key, value, deleted] of entries) {
      if (deleted) {
        resp.patch.push({
          op: "del",
          key,
        });
      } else {
        resp.patch.push({
          op: "put",
          key,
          value,
        });
      }
    }

    const response = JSON.stringify(resp, null, "");
    lc.debug?.(`Returning`, response);
    return response;
  } catch (e) {
    lc.error?.(e);
    throw e;
  }
}
