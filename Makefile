.PHONY: build up down producer-image analyzer-image unit unit-analysis smoke config phase2-smoke phase3-smoke phase4-smoke phase5-smoke suite-smoke analyze-all preflight
build:
	docker compose run --rm builder -B verify
producer-image:
	docker compose build producer
analyzer-image:
	docker compose build analyzer
unit-analysis:
	docker compose run --rm --entrypoint python analyzer -m unittest discover -s tests -v
config:
	docker compose config --quiet
up:
	docker compose up -d --wait kafka jobmanager taskmanager
	docker compose run --rm topic-init
down:
	docker compose down
unit:
	python3 -m unittest discover -s tests -v
smoke:
	python3 scripts/smoke_test.py
phase2-smoke:
	python3 scripts/run_once.py --mode smoke
phase3-smoke:
	python3 scripts/run_experiment.py --experiment 1 --mode smoke
	python3 scripts/run_experiment.py --experiment 2 --mode smoke
phase4-smoke:
	python3 scripts/run_experiment.py --experiment 3 --mode smoke
	python3 scripts/run_experiment.py --experiment 4 --mode smoke
phase5-smoke:
	python3 scripts/run_experiment.py --experiment 5 --mode smoke
suite-smoke:
	python3 scripts/run_experiment.py --experiment all --mode smoke --config configs/experiments/suite-quick-smoke.json
analyze-all:
	docker compose run --rm analyzer --experiment all --mode smoke
preflight:
	python3 scripts/preflight.py
