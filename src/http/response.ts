interface ResponseLimit {
  maxBytes: number;
  error: () => Error;
}

/** Read through EOF, even when fetch loses forwarding of the caller's abort. */
export async function readResponseBytes(
  response: Response,
  signal: AbortSignal,
  limit?: ResponseLimit,
): Promise<Buffer> {
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  const cancel = (reason: unknown) => {
    // cancel() settles pending reads synchronously. Source cleanup may reject
    // or never finish, so it must not delay or replace the primary outcome.
    void reader?.cancel(reason).catch(() => {});
  };
  const onAbort = () => cancel(signal.reason);
  signal.addEventListener("abort", onAbort, { once: true });
  try {
    reader = response.body?.getReader();
    signal.throwIfAborted();
    const chunks: Uint8Array[] = [];
    let size = 0;
    while (reader) {
      const { done, value } = await reader.read();
      // Cancellation also yields EOF. A valid JSON prefix without network EOF
      // must never become a successful response after the caller aborts.
      signal.throwIfAborted();
      if (done) break;
      size += value.length;
      if (limit && size > limit.maxBytes) throw limit.error();
      chunks.push(value);
    }
    return Buffer.concat(chunks);
  } catch (error) {
    const reason = signal.aborted ? signal.reason : error;
    cancel(reason);
    throw reason;
  } finally {
    signal.removeEventListener("abort", onAbort);
    reader?.releaseLock();
  }
}
