import React from "react";
const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

export async function sendMessage({ sessionId, userId, message, lat, lng }) {
  const res = await fetch(`${API_BASE}/v1/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: sessionId, user_id: userId, message, lat, lng })
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Chat failed: ${res.status} ${text}`);
  }
  return res.json(); // { reply }
}

export async function health() {
  const res = await fetch(`${API_BASE}/health`);
  return res.ok;
}
