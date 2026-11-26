import hashlib

from server.ray_app import auth


def test_verify_password_hashes_missing_accounts_to_avoid_user_enumeration(
    monkeypatch,
):
    calls = []
    real_pbkdf2 = hashlib.pbkdf2_hmac

    def spy(name, password, salt, iterations):
        calls.append((name, iterations))
        return real_pbkdf2(name, password, salt, iterations)

    monkeypatch.setattr(auth.hashlib, "pbkdf2_hmac", spy)

    assert auth.verify_password("secure password", None) is False
    assert calls == [("sha256", auth.PASSWORD_ITERATIONS)]


def test_verify_password_rejects_a_missing_password_without_hashing():
    assert auth.verify_password(None, None) is False
