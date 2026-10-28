import mineflayer, { type Bot } from 'mineflayer'

export function createBot(
  opts: {
    host: string;
    port: number;
    username: string;
  }
  ): Promise<Bot> {
  return new Promise((resolve, reject) => {
    const bot = mineflayer.createBot({
      host: opts.host,
      port: opts.port,
      username: opts.username,
      auth: "offline"
    })
    bot.once("spawn", () => {
      resolve(bot)
      console.log(`Bot ${opts.username} has spawned.`)
    })
    bot.once('end', () => {
      console.log(`XoXo from bot ${opts.username}.`)
      process.exit(0)
    })
  })
}

