import { join } from "node:path";
import { RemoteEnvironmentBroker } from "../environments/remote.js";
import { FeishuSource, cachedAccessTokenProvider } from "../sources/feishu.js";
export function envReference(value: unknown): string {
  if (typeof value !== "string" || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(value))
    throw new Error("Expected environment variable reference");
  return value;
}
export function envSecret(value: unknown): string {
  const name = envReference(value),
    secret = process.env[name];
  if (!secret?.trim())
    throw new Error(`Required environment reference is unset: ${name}`);
  return secret;
}
export function configObject(
  value: unknown,
  keys: string[],
  label: string,
): Record<string, any> {
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    Object.keys(value).some((key) => !keys.includes(key))
  )
    throw new Error(
      `Invalid ${label} config; credentials must use environment references`,
    );
  return value as Record<string, any>;
}
export function configuredRemote(
  value: unknown,
  directory: string,
): RemoteEnvironmentBroker | undefined {
  if (value === undefined) return;
  const config = configObject(
    value,
    [
      "endpointId",
      "tokenEnv",
      "directory",
      "transport",
      "url",
      "allowInsecureLoopback",
      "timeoutMs",
    ],
    "remote environment",
  );
  if (
    config.allowInsecureLoopback !== undefined &&
    typeof config.allowInsecureLoopback !== "boolean"
  )
    throw new Error("Invalid remote loopback option");
  return new RemoteEnvironmentBroker({
    ...config,
    directory: config.directory ?? join(directory, "remote-transport"),
    token: envSecret(config.tokenEnv),
    authorize: async () => {
      throw new Error("Remote controller authority not bound");
    },
  } as any);
}
export function configuredFeishu(value: unknown): FeishuSource | undefined {
  if (value === undefined) return;
  const config = configObject(
    value,
    [
      "sourceId",
      "verificationTokenEnv",
      "encryptKeyEnv",
      "appId",
      "chats",
      "trustedUsers",
      "trustedBots",
      "ownSenderIds",
      "enabled",
      "listenedScopes",
      "addressed",
      "accessTokenEnv",
      "accessTokenExpiresAtEnv",
      "apiBase",
      "allowInsecureLoopback",
      "replayWindowMs",
      "timeoutMs",
    ],
    "Feishu",
  );
  if (!!config.accessTokenEnv !== !!config.accessTokenExpiresAtEnv)
    throw new Error(
      "Feishu token provider requires both token and expiry environment references",
    );
  if (config.accessTokenEnv) {
    envReference(config.accessTokenEnv);
    envReference(config.accessTokenExpiresAtEnv);
  }
  const {
    verificationTokenEnv,
    encryptKeyEnv,
    accessTokenEnv,
    accessTokenExpiresAtEnv,
    ...options
  } = config;
  const accessTokenProvider = accessTokenEnv
    ? cachedAccessTokenProvider(async () => ({
        token: envSecret(accessTokenEnv),
        expiresAt: Number(envSecret(accessTokenExpiresAtEnv)),
      }))
    : undefined;
  return new FeishuSource({
    ...options,
    verificationToken: envSecret(verificationTokenEnv),
    encryptKey: envSecret(encryptKeyEnv),
    ...(accessTokenProvider ? { accessTokenProvider } : {}),
  } as any);
}
