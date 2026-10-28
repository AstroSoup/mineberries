.PHONY: run-bot build-bot

build-bot:
	tsc
run-bot: build-bot
	node ./build/libs/dist/main.js $(ARGS)
clean-bot:
	rm -rf ./build/libs/dist