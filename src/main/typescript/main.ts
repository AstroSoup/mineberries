import { createBot } from "./bot.js";
import { parseArgs } from "node:util"

const { values } = parseArgs({
  options: {
    host: { type: "string", default: "localhost" },
    port: { type: "string", default: "25565" },
    username: { type: "string", short: "u", default: "test_bot" },
    password: {type: "string", short: "p", default: "test_pswd"}
  },
})

const port = Number(values.port)
const host = values.host
const username = values.username
const password = values.password

const bot = await createBot(
  {
    host: host,
    port: port,
    username: username,
    password: password
  }
)
bot.chat("Hello world!")
let player = "Godot__"
bot.chat(`/msg ${player} Hello ${player}`)
bot.end()
