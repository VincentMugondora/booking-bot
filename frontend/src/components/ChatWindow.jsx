import { useEffect, useRef, useState } from "react";
import { sendMessage } from "../api/chat";

function uid(prefix) {
  return `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
}

export default function ChatWindow() {
  const [sessionId] = useState(() => {
    const s = localStorage.getItem("chat.sessionId");
    if (s) return s;
    const v = uid("s");
    localStorage.setItem("chat.sessionId", v);
    return v;
  });
  const [userId] = useState(() => {
    const s = localStorage.getItem("chat.userId");
    if (s) return s;
    const v = uid("u");
    localStorage.setItem("chat.userId", v);
    return v;
  });
  const [messages, setMessages] = useState(() => {
    try {
      return JSON.parse(localStorage.getItem("chat.messages") || "[]");
    } catch {
      return [];
    }
  });
  const [input, setInput] = useState("");
  const [pending, setPending] = useState(false);
  const bottomRef = useRef(null);

  useEffect(() => {
    localStorage.setItem("chat.messages", JSON.stringify(messages));
  }, [messages]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, pending]);

  async function onSend() {
    const text = input.trim();
    if (!text || pending) return;
    setInput("");
    setMessages((m) => [...m, { role: "user", text }]);
    setPending(true);
    try {
      const { reply } = await sendMessage({ sessionId, userId, message: text });
      setMessages((m) => [...m, { role: "assistant", text: reply }]);
    } catch (e) {
      setMessages((m) => [
        ...m,
        { role: "assistant", text: "Sorry, something went wrong. Please try again." },
      ]);
    } finally {
      setPending(false);
    }
  }

  function onKeyDown(e) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      onSend();
    }
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-sky-50 via-white to-indigo-50 flex items-center justify-center p-4">
      <div className="w-full max-w-4xl h-[90vh] md:h-[85vh] grid grid-rows-[auto_1fr_auto] rounded-3xl border border-white/40 bg-white/70 backdrop-blur-xl shadow-2xl">
        <Header />
        <MessageList messages={messages} pending={pending} bottomRef={bottomRef} />
        <Composer
          input={input}
          setInput={setInput}
          onSend={onSend}
          onKeyDown={onKeyDown}
          pending={pending}
        />
      </div>
    </div>
  );
}

function Header() {
  return (
    <div className="px-5 py-4 border-b border-white/60 flex items-center justify-between bg-white/60 rounded-t-3xl">
      <div className="flex items-center gap-3">
        <div className="w-10 h-10 rounded-full bg-indigo-600 text-white grid place-items-center font-semibold shadow-md">BA</div>
        <div>
          <div className="font-semibold text-gray-900">Booking Assistant</div>
          <div className="text-xs text-gray-500">Ask me to find and book providers</div>
        </div>
      </div>
      <div className="text-xs text-gray-500">Online</div>
    </div>
  );
}

function MessageList({ messages, pending, bottomRef }) {
  return (
    <div className="overflow-y-auto px-3 sm:px-5 py-4 space-y-3">
      {messages.map((m, i) => (
        <Bubble key={i} role={m.role} text={m.text} />
      ))}
      {pending && <TypingBubble />}
      <div ref={bottomRef} />
    </div>
  );
}

function Bubble({ role, text }) {
  const isUser = role === "user";
  return (
    <div className={`flex ${isUser ? "justify-end" : "justify-start"}`}>
      {!isUser && (
        <div className="hidden sm:flex items-start mr-2">
          <div className="w-8 h-8 rounded-full bg-indigo-600 text-white grid place-items-center text-xs font-semibold">BA</div>
        </div>
      )}
      <div
        className={`max-w-[80%] md:max-w-[70%] rounded-2xl px-4 py-3 text-sm leading-relaxed shadow-md ${
          isUser
            ? "bg-gradient-to-br from-indigo-600 to-violet-600 text-white rounded-tr-sm"
            : "bg-white/80 text-gray-800 border border-gray-200 rounded-tl-sm"
        }`}
      >
        {text}
      </div>
      {isUser && <div className="w-8" />}
    </div>
  );
}

function TypingBubble() {
  return (
    <div className="flex justify-start">
      <div className="hidden sm:flex items-start mr-2">
        <div className="size-8 rounded-full bg-indigo-600 text-white grid place-items-center text-xs font-semibold">BA</div>
      </div>
      <div className="max-w-[70%] rounded-2xl px-4 py-3 text-sm leading-relaxed shadow-md bg-white/80 text-gray-800 border border-gray-200 rounded-tl-sm">
        <div className="flex items-center gap-1">
          <Dot delay="0s" />
          <Dot delay="0.15s" />
          <Dot delay="0.3s" />
        </div>
      </div>
    </div>
  );
}

function Dot({ delay }) {
  return (
    <span
      className="w-2 h-2 bg-gray-400 rounded-full inline-block animate-bounce"
      style={{ animationDelay: delay }}
    />
  );
}

function Composer({ input, setInput, onSend, onKeyDown, pending }) {
  return (
    <div className="p-3 sm:p-4 border-t border-white/60 bg-white/60 rounded-b-3xl">
      <div className="flex gap-2">
        <textarea
          className="flex-1 resize-none rounded-2xl border border-gray-300 bg-white/90 px-4 py-3 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent max-h-40 h-12"
          placeholder="Ask for a service, location, and time..."
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={onKeyDown}
          disabled={pending}
        />
        <button
          className="h-12 whitespace-nowrap px-5 rounded-2xl bg-indigo-600 text-white font-medium shadow-md hover:bg-indigo-700 active:bg-indigo-800 disabled:opacity-50 disabled:cursor-not-allowed"
          onClick={onSend}
          disabled={pending || !input.trim()}
        >
          Send
        </button>
      </div>
    </div>
  );
}
