PGBIN := $(HOME)/Applications/Postgres.app/Contents/Versions/17/bin
PGDATA := .pgdata
PGPORT := 5434

.PHONY: test pg pg-stop pg-reset migrate tunnel

tunnel:                 ## public URL for /webhooks/fathom + /slack/interactions
	@PATH="$(HOME)/bin:$$PATH" command -v cloudflared >/dev/null || { \
	  echo "cloudflared missing — captain approved the direct download 2026-07-31;"; \
	  echo "re-fetch: curl -sL -o ~/bin/cloudflared.tgz https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-darwin-arm64.tgz"; \
	  exit 1; }
	PATH="$(HOME)/bin:$$PATH" cloudflared tunnel --url http://localhost:8787

test:
	uv run pytest

pg:                     ## init (once) and start the local cluster on :5434
	@test -d $(PGDATA) || $(PGBIN)/initdb -D $(PGDATA) -U relay_superagent --auth=trust -E UTF8
	@$(PGBIN)/pg_ctl -D $(PGDATA) -o "-p $(PGPORT) -k /tmp" -l $(PGDATA)/log start || true
	@sleep 1
	@$(PGBIN)/createdb -h localhost -p $(PGPORT) -U relay_superagent relay_superagent 2>/dev/null || true
	@$(MAKE) migrate

pg-stop:
	@$(PGBIN)/pg_ctl -D $(PGDATA) stop

pg-reset: pg-stop
	rm -rf $(PGDATA) && $(MAKE) pg

migrate:
	@$(PGBIN)/psql -h localhost -p $(PGPORT) -U relay_superagent -d relay_superagent -q -f migrations/001_init.sql
