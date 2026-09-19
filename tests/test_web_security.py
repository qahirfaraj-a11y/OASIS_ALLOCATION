"""The web console on a website: sign-in, permissions, and the public demo.

It shipped with no login, safe only while bound to 127.0.0.1 — which a reverse
proxy undoes. These pin what must hold once it faces the internet.
"""
import pytest
from fastapi.testclient import TestClient

from oasis.web import security as S

HDR = {S.CLIENT_HEADER: "web"}


def _perms(role):
    from oasis.logic.auth_manager import get_user_permissions
    return get_user_permissions(role)


ADMIN = {"username": "ops", "display_name": "Ops", "role": "ops_admin", "assigned_org": None,
         "permissions": _perms("ops_admin")}
BRANCH = {"username": "bm", "display_name": "Branch", "role": "branch_manager", "assigned_org": "ORG001",
          "permissions": _perms("branch_manager")}


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    monkeypatch.delenv("OASIS_WEB_PUBLIC", raising=False)
    S.LIMITER.reset()
    yield
    S.LIMITER.reset()


def client(monkeypatch, user=None):
    from oasis.web.app import app
    monkeypatch.setattr(S, "resolve_user", lambda sid, root: user)
    return TestClient(app)


# ── private mode: nothing without a session ─────────────────────────────────
class TestSignIn:
    @pytest.mark.parametrize("path", ["/api/sites", "/api/health", "/api/sites/readiness",
                                      "/api/sites/map", "/api/me"])
    def test_every_read_needs_a_session(self, monkeypatch, path):
        assert client(monkeypatch).get(path).status_code == 401

    def test_writes_need_a_session_too(self, monkeypatch):
        r = client(monkeypatch).post("/api/orders/ORG001/push", json={"rows": [{"x": 1}]}, headers=HDR)
        assert r.status_code == 401

    def test_a_request_from_another_site_is_refused(self, monkeypatch):
        # a cross-site form cannot set the console's header
        r = client(monkeypatch, ADMIN).post("/api/jobs/transfers")
        assert r.status_code == 403

    def test_the_real_login_flow(self, tmp_path, monkeypatch):
        from oasis.logic.auth_manager import seed_users, set_password
        from oasis.logic.db_connector import ensure_oasis_tables
        from oasis.web.app import app
        db = tmp_path / "store.db"
        monkeypatch.setenv("OASIS_DB_PATH", str(db))
        monkeypatch.setenv("OASIS_SEED_PASSWORD", "install-pass-2026")
        ensure_oasis_tables(str(db))
        seed_users(str(db))
        c = TestClient(app)
        # the installation password is refused on the web
        r = c.post("/api/login", json={"username": "ops_admin", "password": "install-pass-2026"}, headers=HDR)
        assert r.status_code == 403 and "installation password" in r.json()["detail"]
        set_password(str(db), "ops_admin", "a-much-better-one-2026")
        r = c.post("/api/login", json={"username": "ops_admin", "password": "wrong"}, headers=HDR)
        assert r.status_code == 401
        r = c.post("/api/login", json={"username": "ops_admin", "password": "a-much-better-one-2026"}, headers=HDR)
        assert r.status_code == 200
        cookie = r.headers["set-cookie"].lower()
        assert "httponly" in cookie and "samesite=strict" in cookie
        assert c.get("/api/me").json()["user"]["username"] == "ops_admin"
        assert c.post("/api/logout", headers=HDR).status_code == 200
        assert c.get("/api/me").status_code == 401        # the session is revoked, not just forgotten

    def test_login_attempts_are_limited(self, monkeypatch):
        c = client(monkeypatch)
        codes = [c.post("/api/login", json={"username": "x", "password": "y"}, headers=HDR).status_code
                 for _ in range(S.LOGIN_LIMIT[0] + 1)]
        assert codes[-1] == 429


class TestPermissions:
    def test_a_branch_manager_sees_only_their_store(self, monkeypatch):
        from oasis.desktop import data as D
        monkeypatch.setattr(D, "list_stores", lambda root=None: [{"org_cd": "ORG001", "name": "A"},
                                                                 {"org_cd": "ORG002", "name": "B"}])
        sites = client(monkeypatch, BRANCH).get("/api/sites").json()["sites"]
        assert [s["org_cd"] for s in sites] == ["ORG001"]

    def test_nor_can_they_run_another_stores_order(self, monkeypatch):
        r = client(monkeypatch, BRANCH).post("/api/jobs/orders/ORG002", headers=HDR)
        assert r.status_code == 403

    def test_nor_read_its_job_by_id(self, monkeypatch):
        from oasis.web import jobs
        job = jobs.submit("orders", "ORG002", lambda: {"rows": ["secret"]})
        r = client(monkeypatch, BRANCH).get(f"/api/jobs/{job.id}")
        assert r.status_code == 404

    @pytest.mark.parametrize("path,body", [
        ("/api/orders/ORG001/push", {"rows": [{"x": 1}]}),   # can_approve_po
        ("/api/sites/place", {"csv": "a"}),                   # can_edit_config
        ("/api/sites/competitor", {"action": "add"}),
        ("/api/jobs/transfers", None),                        # transfer tab
    ])
    def test_role_limits_hold(self, monkeypatch, path, body):
        r = client(monkeypatch, BRANCH).post(path, json=body, headers=HDR)
        assert r.status_code == 403

    def test_the_audit_trail_names_the_signed_in_user(self, monkeypatch):
        from oasis.desktop import data as D
        seen = {}

        def fake_push(org, who, rows, root=None):
            seen["who"] = who
            return {"success": True, "pushed_count": len(rows)}
        monkeypatch.setattr(D, "push_purchase_order", fake_push)
        r = client(monkeypatch, ADMIN).post("/api/orders/ORG001/push",
                                            json={"rows": [{"x": 1}], "username": "mallory"}, headers=HDR)
        assert r.status_code == 200 and seen["who"] == "ops"


# ── the public demo ──────────────────────────────────────────────────────────
@pytest.fixture
def public(monkeypatch):
    from oasis.desktop import data as D
    monkeypatch.setenv("OASIS_WEB_PUBLIC", "1")
    from oasis.logic import db
    for v in ("OASIS_POS_DB_URL", "OASIS_DB_URL", "OASIS_ERP"):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setattr(D, "data_provenance", lambda root=None: {"source": "demo", "is_sample": True})
    monkeypatch.setattr(db, "onboarded_pos_url", lambda: None)
    return monkeypatch


class TestThePublicDemo:
    def test_it_refuses_to_start_on_real_data(self, public):
        from oasis.desktop import data as D
        from oasis.web.app import app
        public.setattr(D, "data_provenance", lambda root=None: {"source": "pos", "is_sample": False})
        with pytest.raises(RuntimeError, match="sample store only"):
            with TestClient(app):
                pass

    def test_nor_with_a_live_pos_configured(self, public):
        from oasis.web.app import app
        public.setenv("OASIS_POS_DB_URL", "mssql+pyodbc://u:p@host/db")
        with pytest.raises(RuntimeError, match="OASIS_POS_DB_URL"):
            with TestClient(app):
                pass

    def test_nor_on_an_install_whose_wizard_connected_a_pos(self, public):
        from oasis.logic import db
        from oasis.web.app import app
        public.setattr(db, "onboarded_pos_url", lambda: "mssql+pyodbc://u:p@host/db")
        with pytest.raises(RuntimeError, match="live POS connected"):
            with TestClient(app):
                pass

    def test_reads_work_without_an_account(self, public):
        from oasis.web.app import app
        with TestClient(app) as c:
            assert c.get("/api/me").json() == {"public": True, "user": None}
            assert c.get("/api/sites/readiness").status_code == 200

    @pytest.mark.parametrize("path", ["/api/orders/ORG001/push", "/api/sites/place",
                                      "/api/sites/region", "/api/sites/competitor"])
    def test_nothing_can_be_changed_or_fetched(self, public, path):
        from oasis.web.app import app
        with TestClient(app) as c:
            assert c.post(path, json={}, headers=HDR).status_code == 403

    def test_there_are_no_accounts(self, public):
        from oasis.web.app import app
        with TestClient(app) as c:
            assert c.post("/api/login", json={"username": "a", "password": "b"}, headers=HDR).status_code == 404

    def test_heavy_work_is_rate_limited(self, public):
        from oasis.web.app import app
        with TestClient(app) as c:
            codes = [c.post("/api/jobs/suggest", json={}, headers=HDR).status_code for _ in range(7)]
        assert codes[:6] == [200] * 6 and codes[6] == 429


# ── secrets never reach a browser ────────────────────────────────────────────
class TestScrubbing:
    @pytest.mark.parametrize("raw,secret", [
        ("(pyodbc) mssql+pyodbc://oasis_ro:Hunter2@10.0.0.5/RXL failed", "Hunter2"),
        ("DRIVER={ODBC};SERVER=x;UID=sa;PWD=Hunter2;", "Hunter2"),
        ("postgresql://svc:Hunter2@db:5432/oasis", "Hunter2"),
    ])
    def test_connection_strings_are_scrubbed(self, raw, secret):
        out = S.scrub(raw)
        assert secret not in out and "<redacted>" in out

    def test_scrubbing_is_applied_to_responses(self, monkeypatch):
        from oasis.desktop import data as D

        def boom(root=None):
            raise RuntimeError("login failed for mssql+pyodbc://oasis_ro:Hunter2@10.0.0.5/RXL")
        monkeypatch.setattr(D, "region_data_status", boom)
        r = client(monkeypatch, ADMIN).get("/api/sites/readiness")
        assert "Hunter2" not in r.text and "<redacted>" in r.text
