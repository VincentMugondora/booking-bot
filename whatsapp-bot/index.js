import 'dotenv/config'
import makeWASocket, { useMultiFileAuthState, fetchLatestBaileysVersion, DisconnectReason } from '@whiskeysockets/baileys'
import axios from 'axios'
import pino from 'pino'
import qrcode from 'qrcode-terminal'
import fs from 'node:fs/promises'

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
    logger: pino({ level: 'info' })
  })

  sock.ev.on('creds.update', saveCreds)

  sock.ev.on('connection.update', (update) => {
    const { connection, lastDisconnect, qr } = update
    if (qr) {
      console.log('Scan this QR with WhatsApp (Linked Devices):')
      qrcode.generate(qr, { small: true })
    }
    if (connection === 'open') {
      console.log('✅ WhatsApp connected')
    } else if (connection === 'close') {
      const code = lastDisconnect?.error?.output?.statusCode || lastDisconnect?.error?.statusCode || lastDisconnect?.error?.code
      const shouldReconnect = code !== DisconnectReason.loggedOut
      console.log('Connection closed. code=', code, 'reconnect=', shouldReconnect)
      if (shouldReconnect) {
        setTimeout(() => startBot().catch(() => {}), 2000)
      } else {
        console.log(`❌ Logged out. Delete the '${SESSION_DIR}' folder and run again to re-link.`)
      }
    }
  })

  sock.ev.on('messages.upsert', async (msgUpdate) => {
    if (msgUpdate.type !== 'notify') return
    for (const msg of msgUpdate.messages) {
      if (!msg.message || msg.key.fromMe) continue
      const chatId = msg.key.remoteJid
      // Ignore group chats
      if (chatId.endsWith('@g.us')) continue
      const text = extractText(msg).trim()
      if (!text) continue

      // Special command: /unlink — logout & clear session directory
      if (text === '/unlink') {
        try {
          await sock.sendMessage(chatId, { text: 'Unlinking session... You may need to restart the bot and scan the QR again.' }, { quoted: msg })
        } catch {}
        try { await sock.logout() } catch {}
        try { await fs.rm(SESSION_DIR, { recursive: true, force: true }) } catch {}
        try {
          await sock.sendMessage(chatId, { text: '✅ Unlinked. Restart the bot (npm start) to display a new QR and relink.' })
        } catch {}
        continue
      }

      try {
        // show typing while we process
        try { await sock.presenceSubscribe(chatId); await sock.sendPresenceUpdate('composing', chatId) } catch {}
        const payload = {
          session_id: chatId,
          user_id: chatId,
          message: text,
          fast: true
        }
        const axiosPromise = axios.post(`${BACKEND_URL}/v1/chat`, payload, { timeout: 60000, headers: { 'Content-Type': 'application/json' } })
        let ackSent = false
        await Promise.race([
          axiosPromise,
          (async () => { await new Promise(r => setTimeout(r, 1500)); ackSent = true; try { await sock.sendMessage(chatId, { text: '⏳ One sec…' }, { quoted: msg }) } catch {} })()
        ])
        const res = await axiosPromise
        const reply = res?.data?.reply || 'Sorry, I could not process that.'
        await sock.sendMessage(chatId, { text: reply }, { quoted: msg })
      } catch (err) {
        const detail = err?.response?.data || err.message || String(err)
        await sock.sendMessage(chatId, { text: `Error from server: ${JSON.stringify(detail).slice(0, 500)}` }, { quoted: msg })
      } finally {
        try { await sock.sendPresenceUpdate('paused', chatId) } catch {}
      }
    }
  })
}

startBot().catch((e) => {
  // eslint-disable-next-line no-console
  console.error('WhatsApp bot failed to start:', e)
})
