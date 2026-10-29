// This file will have all supervisor functions.
import { type Bot } from "mineflayer"

/*
  New functionality should be added through plugins, per Mineflayer docs.

  Reference:
  https://github.com/PrismarineJS/mineflayer/blob/master/docs/api.md#botloadpluginplugin
*/ 
declare module "mineflayer" {
  interface Bot {
    startInventorisation(): void;
    sendCode(to: string): void;

  }
}

export function supervisorPlugin(bot: Bot) {
  bot.startInventorisation = () => {

  }
  bot.sendCode = () => {

  }
}
