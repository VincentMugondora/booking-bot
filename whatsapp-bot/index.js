import 'dotenv/config'
import makeWASocket, { useMultiFileAuthState, fetchLatestBaileysVersion } from '@whiskeysockets/baileys'
import axios from 'axios'
import pino from 'pino'

const BACKEND_URL = process.env.BACKEND_URL || 'http://127.0.0.1:8000'
const SESSION_DIR = process.env.SESSION_DIR || 'auth_info'

function extractText(msg) {
  const m = msg.message || {}
  if (m.conversation) return m.conversation
  if (m.extendedTextMessage?.text) return m.extendedTextMessage.text
  if (m.imageMessage?.caption) return m.imageMessage.caption
  if (m.videoMessage?.caption) return m.videoMessage.caption
  if (m.ephemeralMessage?.message?.extendedTextMessage?.text) return m.ephemeralMessage.message.extendedTextMessage.text
  return ''
}

async function startBot() {
  const { state, saveCreds } = await useMultiFileAuthState(SESSION_DIR)
  const { version } = await fetchLatestBaileysVersion()
  const sock = makeWASocket({
    version,
    auth: state,
    printQRInTerminal: true,
    logger: pino({ level: 'info' })
  })

  sock.ev.on('creds.update', saveCreds)

  sock.ev.on('messages.upsert', async (msgUpdate) => {
    if (msgUpdate.type !== 'notify') return
    for (const msg of msgUpdate.messages) {
      if (!msg.message || msg.key.fromMe) continue
      const chatId = msg.key.remoteJid
      const text = extractText(msg).trim()
      if (!text) continue

      try {
        const payload = {
          session_id: chatId,
          user_id: chatId,
          message: text
        }
        const res = await axios.post(`${BACKEND_URL}/v1/chat`, payload, {
          timeout: 20000,
          headers: { 'Content-Type': 'application/json' }
        })
        const reply = res?.data?.reply || 'Sorry, I could not process that.'
        await sock.sendMessage(chatId, { text: reply }, { quoted: msg })
      } catch (err) {
        const detail = err?.response?.data || err.message || String(err)
        await sock.sendMessage(chatId, { text: `Error from server: ${JSON.stringify(detail).slice(0, 500)}` }, { quoted: msg })
      }
    }
  })
}

startBot().catch((e) => {
  // eslint-disable-next-line no-console
  console.error('WhatsApp bot failed to start:', e)
})
