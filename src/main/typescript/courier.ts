// This file will have all courier functions.
import { type Bot } from "mineflayer"
import { Vec3 } from "vec3"

/*
  New functionality should be added through plugins, per Mineflayer docs.

  Reference:
  https://github.com/PrismarineJS/mineflayer/blob/master/docs/api.md#botloadpluginplugin
*/ 
declare module "mineflayer" {
  interface Bot {
    deliver(from: Vec3, to: Vec3): void;
  }
}

export function courierPlugin(bot: Bot) {
  bot.deliver = () => {

  }
}