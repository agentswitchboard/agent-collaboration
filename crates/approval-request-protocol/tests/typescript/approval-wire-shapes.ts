import type {
  DecideReply,
  Delivery,
  OptionLifetime,
  PrivateEnding,
  QuestionAnswerValue,
  Raise,
  StopRequest,
} from "../../contracts/approval-requests";

const session = "01934a50-1000-7000-8000-000000000003";

export const turnMemory = { kind: "thisTurn", kept: "agentMemory" } satisfies OptionLifetime;
export const cancelledProviderQuestion = {
  kind: "providerQuestion",
  ending: { kind: "cancelled", reason: { kind: "stopped", cause: "askWithdrawn" } },
} satisfies PrivateEnding;
export const checkedNumericAnswer = { kind: "number", value: "3" } satisfies QuestionAnswerValue;
export const received = { kind: "receivedByHolder" } satisfies Delivery;

export const providerLocalRefusalStop = {
  kind: "provider",
  requestId: session,
  routerRun: session,
  contentsDigest: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  cause: "holderRefusedLocally",
} satisfies StopRequest;
export const parkedLocalRefusalStop = {
  ...providerLocalRefusalStop,
  kind: "unprovenCall",
} satisfies StopRequest;

// @ts-expect-error A verified turn cannot retain a file rule.
export const invalidTurn: OptionLifetime = { kind: "thisTurn", kept: "agentRulesFile" };

// @ts-expect-error A provider question cannot disclose a numeric approval option.
export const invalidQuestionEnding: PrivateEnding = { kind: "providerQuestion", ending: { kind: "decided", answer: 1, by: { kind: "session", session } } };

// @ts-expect-error TimedOut cannot stand in for a saved decision.
export const invalidDecideSuccess: DecideReply = { kind: "savedDecision", decision: { kind: "timedOut" } };

// @ts-expect-error Only named, tagged question answer objects are allowed.
export const invalidAnswerValue: QuestionAnswerValue = true;

// @ts-expect-error The smaller configuration stores only one of three delivery states.
export const invalidDelivery: Delivery = { kind: "reported" };

// @ts-expect-error The provider holder takes a Central session ID, not an addressed harness ID.
export const invalidHolder: Extract<Raise, { kind: "providerApproval" }>["holder"] = { sessionId: { session }, routerRun: session };

export const parkedStart = {
  kind: "unprovenCall",
  requestId: session,
  holder: { routerRun: session },
  offer: {
    operation: { kind: "startSession", agentProgram: "claude", workingFolder: "/project", mode: "plan", workFolders: ["/project/docs", "/project/src"] },
  },
  invocation: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
} satisfies Raise;

// @ts-expect-error A parked holder names a run without attributing a session.
export const invalidParkedHolder: Extract<Raise, { kind: "unprovenCall" }>["holder"] = { routerRun: session, sessionId: session };

// @ts-expect-error C1 cannot carry a general operation.
export const invalidParkedOperation: Extract<Raise, { kind: "unprovenCall" }>["offer"]["operation"] = { kind: "runCommand" };
