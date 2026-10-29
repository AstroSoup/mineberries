import mineflayer, { type Bot } from "mineflayer"

const AUTH_REGEX = /\/(login|register)\b/i

export function createBot(
  opts: {
    host: string;
    port: number;
    username: string;
    password?: string;
  }
  ): Promise<Bot> {
  return new Promise((resolve, reject) => {
    const bot = mineflayer.createBot({
      ...opts,
      auth: "offline"
    })
    
    bot.once("spawn", async () => {
      console.log(`Bot ${opts.username} has spawned.`)
      if (opts.password !== undefined) {
        console.log("Trying to authenticate with password...")
        const server_prompt = await awaitSystemMessage(bot, AUTH_REGEX) 
        if (server_prompt.includes("/login")) {
          bot.chat(`/login ${opts.password}`)
          console.log("Successful login.")
        } else if (server_prompt.includes("/register")) {
          bot.chat(`/register ${opts.password} ${opts.password}`)
          console.log("Successful registration.")
        } else { 
          console.log("Something went wrong. Could not authenticate.")
        }
      }  
      // sleeping to ensure that AuthMe wont kick the bot out.
      await new Promise(r => setTimeout(r, 2000));
      resolve(bot)
    })
    bot.once('end', () => {
      console.log(`XoXo from bot ${opts.username}.`)
      process.exit(0)
    })
  })
}

function awaitSystemMessage(
  bot: Bot,
  match: string | RegExp,
  timeoutMs = 5000
): Promise<string> {
  return new Promise((resolve, reject) => {
    const onMessage = (message: string, position: string) => {
      if (position !== "system") return;
      const ok = typeof match === "string" ? message.includes(match) : match.test(message);
      if (!ok) return;
      cleanup();
      resolve(message);
    };

    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`Timed out waiting for system message: ${match}`));
    }, timeoutMs);

    const cleanup = () => {
      clearTimeout(timer);
      bot.off("messagestr", onMessage);
    };

    bot.on("messagestr", onMessage);
  });
}