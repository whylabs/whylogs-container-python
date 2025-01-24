.PHONY: test run build deploy clean all

run:
	npm run dev

build:
	npm run build

build-standalone:
	npm run build:standalone

clean:
	rm -rf ./dist

format:
	npx prettier -c ./src/**/*.ts ./src/**/*.js

format-fix:
	npx prettier --write -c ./src/**/*.ts ./src/**/*.js

lint:
	npx eslint ./src/**/*.ts ./src/**/*.js
