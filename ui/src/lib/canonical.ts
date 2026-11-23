export function canonicalJson(value: unknown): string {
  if (typeof value === "number" && !Number.isFinite(value)) throw new Error("Floats are not allowed");
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

export function ulid(): string {
  const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
  const time = Date.now();
  let encoded = "";
  let value = time;
  for (let i = 0; i < 10; i++) {
    encoded = alphabet[value % 32] + encoded;
    value = Math.floor(value / 32);
  }
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  return encoded + Array.from(bytes, (b) => alphabet[b % 32]).join("").slice(0, 16);
}
