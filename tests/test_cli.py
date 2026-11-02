
import pytest

from webhook_outbox import __version__
from webhook_outbox.cli import main
from webhook_outbox.store import DEAD, SQLiteStore


@pytest.fixture
def db(tmp_path):
    return str(tmp_path / "outbox.db")


def run(argv):
    return main(argv)


def test_version_is_printed_with_the_command_name(capsys):
    with pytest.raises(SystemExit) as exc:
        run(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"outbox {__version__}"


def test_enqueue_reports_a_new_delivery(db, capsys):
    code = run(["--db", db, "enqueue", "--endpoint", "https://x/hook",
                "--event", "charge.succeeded", "--payload", '{"id": 1}'])
    assert code == 0
    assert "queued delivery 1" in capsys.readouterr().out


def test_a_second_enqueue_with_the_same_key_says_so(db, capsys):
    for _ in range(2):
        code = run(["--db", db, "enqueue", "--endpoint", "https://x/hook",
                    "--event", "e", "--payload", "{}", "--key", "k1"])
    assert code == 0
    assert "already queued delivery 1" in capsys.readouterr().out


def test_bad_json_is_a_usage_error(db, capsys):
    code = run(["--db", db, "enqueue", "--endpoint", "https://x/hook",
                "--event", "e", "--payload", "{not json"])
    assert code == 2
    assert "could not read the payload" in capsys.readouterr().err


def test_list_on_an_empty_outbox_is_not_an_error(db, capsys):
    assert run(["--db", db, "list"]) == 0
    assert "no deliveries" in capsys.readouterr().out


def test_list_shows_what_is_queued(db, capsys):
    run(["--db", db, "enqueue", "--endpoint", "https://x/hook", "--event", "e"])
    capsys.readouterr()
    assert run(["--db", db, "list"]) == 0
    out = capsys.readouterr().out
    assert "pending" in out and "https://x/hook" in out


def test_log_reports_the_whole_delivery(db, capsys):
    run(["--db", db, "enqueue", "--endpoint", "https://x/hook",
         "--event", "charge.succeeded", "--payload", '{"id": 7}', "--key", "k1"])
    capsys.readouterr()
    assert run(["--db", db, "log", "1"]) == 0
    out = capsys.readouterr().out
    assert "charge.succeeded" in out and "k1" in out and '"id":7' in out


def test_log_on_a_missing_id_is_a_usage_error(db, capsys):
    assert run(["--db", db, "log", "42"]) == 2
    assert "no delivery with id 42" in capsys.readouterr().err


def test_replay_queues_a_dead_delivery_again(db, capsys):
    store = SQLiteStore(db)
    delivery, _ = store.enqueue("https://x/hook", "e", {})
    store.mark_dead(delivery.id, 404, "HTTP 404")
    store.close()

    assert run(["--db", db, "replay", str(delivery.id)]) == 0
    assert "replay queued for delivery" in capsys.readouterr().out

    check = SQLiteStore(db)
    assert check.get(delivery.id).status == "pending"
    check.close()


def test_replay_on_a_missing_id_is_a_usage_error(db, capsys):
    assert run(["--db", db, "replay", "99"]) == 2
    assert "no delivery with id 99" in capsys.readouterr().err


def test_an_empty_dead_letter_queue_exits_zero(db, capsys):
    assert run(["--db", db, "dead"]) == 0
    assert "dead-letter queue is empty" in capsys.readouterr().out


def test_dead_letters_exit_one_because_they_need_a_human(db, capsys):
    store = SQLiteStore(db)
    delivery, _ = store.enqueue("https://x/hook", "e", {})
    store.mark_dead(delivery.id, 404, "HTTP 404")
    store.close()
    assert run(["--db", db, "dead"]) == 1
    out = capsys.readouterr().out
    assert "1 dead letter" in out and "outbox replay" in out


def test_stats_reports_counts_and_exits_on_dead_letters(db, capsys):
    store = SQLiteStore(db)
    ok, _ = store.enqueue("https://x/hook", "e", {})
    bad, _ = store.enqueue("https://y/hook", "e", {})
    store.mark_delivered(ok.id, 200)
    store.mark_dead(bad.id, 500, "HTTP 500")
    store.close()

    assert run(["--db", db, "stats"]) == 1
    out = capsys.readouterr().out
    assert "delivered  1" in out and "dead       1" in out


def test_run_once_on_an_empty_outbox_is_clean(db, capsys):
    assert run(["--db", db, "run", "--once"]) == 0
    assert capsys.readouterr().out == ""


def test_run_once_against_a_dead_endpoint_records_and_exits_one(db, capsys, tmp_path):
    """A refused connection is retried, not fatal, and the exit code says work is waiting."""
    run(["--db", db, "enqueue", "--endpoint", "http://127.0.0.1:9/hook",
         "--event", "e", "--payload", "{}"])
    capsys.readouterr()
    code = run(["--db", db, "run", "--once"])
    assert code == 1
    out = capsys.readouterr().out
    assert "1 claimed" in out and "waiting" in out
    store = SQLiteStore(db)
    assert store.list(DEAD) == []
    store.close()
