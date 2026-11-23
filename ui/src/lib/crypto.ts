import * as ed from "@noble/ed25519";
import { sha512 } from "@noble/hashes/sha2.js";
import { canonicalJson } from "./canonical";
import type { Envelope, EnvelopeType } from "../types/protocol";

ed.etc.sha512Sync = (...messages) =>
  sha512(ed.etc.concatBytes(...messages));
ed.etc.sha512Async = async (...messages) =>
  sha512(ed.etc.concatBytes(...messages));

interface DeviceKey {
  publicKey: string;
  privateKey: string;
  keyId: string;
}

export function ensureDeviceKeys(senderId?: string): Record<string, DeviceKey> {
  const stored = localStorage.getItem("sd-calendar-device-keys");
  const keys: Record<string, DeviceKey> = stored ? JSON.parse(stored) : {};
  if (senderId && !keys[senderId]) {
    const privateBytes = ed.utils.randomPrivateKey();
    const publicBytes = ed.getPublicKey(privateBytes);
    const privateKey = toHex(privateBytes);
    const publicKey = toHex(publicBytes);
    keys[senderId] = {
      privateKey,
      publicKey,
      keyId: `did:${senderId}:` + publicKey.slice(0, 12)
    };
    localStorage.setItem("sd-calendar-device-keys", JSON.stringify(keys));
  }
  return keys;
}

export async function signEnvelope(envelope: Omit<Envelope, "signature">): Promise<Envelope> {
  const keys = ensureDeviceKeys(envelope.sender_id);
  const key = keys[envelope.sender_id];
  if (!key) throw new Error(`No local key for ${envelope.sender_id}`);
  const signingInput = {
    deployment_id: envelope.deployment_id,
    group_id: envelope.group_id,
    msg_id: envelope.msg_id,
    type: envelope.type,
    uid: envelope.uid,
    sender_id: envelope.sender_id,
    key_id: envelope.key_id,
    sequence: envelope.sequence,
    seen_sequence: envelope.seen_sequence,
    client_sent_at: envelope.client_sent_at,
    payload: envelope.payload
  };
  const message = new TextEncoder().encode(canonicalJson(signingInput));
  const signature = await ed.signAsync(message, key.privateKey);
  return { ...envelope, signature: toHex(signature) };
}

export function toHex(bytes: Uint8Array): string {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

export function keyIdFor(user: string): string {
  return ensureDeviceKeys(user)[user].keyId;
}
