import type {
  DecidePersonReply,
  OfferEnding,
  OptionLifetime,
  QuestionAnswerValue,
} from "../../contracts/approval-requests";

const requestId = "01934a50-1000-7000-8000-000000000001";
const routerRun = "01934a50-1000-7000-8000-000000000002";
const session = {
  machineId: "01934a50-1000-7000-8000-000000000003",
  sessionId: "host/session",
};

export const turnMemory = {
  kind: "thisTurn",
  kept: "agentMemory",
} satisfies OptionLifetime;

export const cancelledProviderQuestion = {
  kind: "providerQuestion",
  requestId,
  holder: { routerRun, session },
  ending: { kind: "cancelled", reason: { kind: "stopped", cause: "askReplaced" } },
} satisfies OfferEnding;

export const checkedNumericAnswer = {
  kind: "number",
  value: 3,
} satisfies QuestionAnswerValue;

// @ts-expect-error A verified turn cannot retain a file rule.
export const invalidTurn: OptionLifetime = { kind: "thisTurn", kept: "agentRulesFile" };

// @ts-expect-error A provider ending cannot have a parked-call stop cause.
export const invalidProviderStop: OfferEnding = { kind: "providerApproval", requestId, holder: { routerRun, session }, ending: { kind: "cancelled", reason: { kind: "stopped", cause: "callWithdrawn" } } };

// @ts-expect-error A provider question cannot disclose a numeric approval option.
export const invalidQuestionEnding: OfferEnding = { kind: "providerQuestion", requestId, holder: { routerRun, session }, ending: { kind: "decided", answer: 1, by: { kind: "session", session } } };

// @ts-expect-error TimedOut is not an accepted answer with attribution.
export const invalidDecideSuccess: DecidePersonReply = { kind: "decided", decision: { kind: "timedOut" } };

// @ts-expect-error Only named, tagged question answer objects are allowed.
export const invalidAnswerValue: QuestionAnswerValue = true;
