import {
  canonicalJson,
  type Mandate,
  type Policy,
  type Rule,
  type Ticket,
} from "../domain/model.js";
import { estimateTokens } from "../runtime/artifacts.js";

/** These summaries come only from trusted domain records. Excerpts always name a full recall reference. */
export function routerAuthority(policy: Policy) {
  const rules: {
    id: string;
    enabled: boolean;
    instructions: string;
    capabilities: string[];
  }[] = [];
  for (const rule of policy.rules) {
    const item = { ...rule, instructions: rule.instructions.slice(0, 160) };
    if (
      rules.length >= 8 ||
      estimateTokens(canonicalJson([...rules, item])) > 2000
    )
      break;
    rules.push(item);
  }
  const explicitCapabilities: string[] = [];
  for (const capability of policy.explicitCapabilities ?? [
    "repo.read",
    "channel.reply",
  ]) {
    if (
      explicitCapabilities.length >= 8 ||
      estimateTokens(canonicalJson([...explicitCapabilities, capability])) > 600
    )
      break;
    explicitCapabilities.push(capability);
  }
  return {
    policyVersion: policy.version,
    acceptMode: policy.acceptMode,
    explicitCapabilities,
    ruleCount: policy.rules.length,
    rules,
    reference: `policy:${policy.version}`,
  };
}
export function ticketIndex(tickets: Ticket[]) {
  const items: {
    id: string;
    goal: string;
    lifecycle: string;
    assignmentEpoch: number;
    reference: string;
  }[] = [];
  for (const ticket of tickets) {
    const item = {
      id: ticket.id,
      goal: ticket.goal.slice(0, 160),
      lifecycle: ticket.lifecycle,
      assignmentEpoch: ticket.assignmentEpoch,
      reference: `ticket_context:${ticket.id}`,
    };
    if (
      items.length >= 8 ||
      estimateTokens(canonicalJson([...items, item])) > 2000
    )
      break;
    items.push(item);
  }
  return { items, count: tickets.length, reference: "tickets" };
}
export interface WorkerAuthority {
  mandate: Omit<Mandate, "rulesSnapshot">;
  matchedRules: Rule[];
  rulesAvailable: boolean;
}
export interface WorkerContextRecord {
  ticketId: string;
  mandateId?: string;
  text: string;
  artifact?: string;
  summary: WorkerAuthority | { excerpt: string; reference: string };
}
export function workerAuthority(
  mandate: Mandate,
  policy?: Policy,
): WorkerAuthority {
  const { rulesSnapshot, ...grant } = mandate;
  const rules =
    rulesSnapshot ??
    (policy?.version === mandate.policyVersion
      ? policy.rules.filter((rule) => mandate.ruleIds.includes(rule.id))
      : undefined);
  return {
    mandate: grant,
    matchedRules: rules ?? [],
    rulesAvailable: mandate.ruleIds.length === 0 || rules !== undefined,
  };
}
