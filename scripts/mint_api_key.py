"""
Mint a long-lived API key for an existing user.

Usage:
    python -m scripts.mint_api_key --username lucas --name "local-dev" --scopes admin
    python -m scripts.mint_api_key --username lucas --name "cronjob-ingest" --scopes write

The full key is printed ONCE. Store it immediately - it cannot be recovered.
By default the key does not expire (pass --expires-days N for a TTL).
"""
import argparse
import sys

from src.core.database import get_db_context, get_user_by_username, create_api_key
from src.auth.auth import generate_api_key, hash_api_key
from src.schemas import VALID_SCOPES


def main():
    parser = argparse.ArgumentParser(description="Mint a long-lived API key")
    parser.add_argument("--username", required=True, help="Existing username to attach the key to")
    parser.add_argument("--name", required=True, help="Human-readable key label (e.g. 'local-dev')")
    parser.add_argument(
        "--scopes",
        nargs="+",
        default=["admin"],
        help=f"Scopes to grant. Valid: {VALID_SCOPES}. Default: admin",
    )
    parser.add_argument(
        "--expires-days",
        type=int,
        default=None,
        help="Days until expiry. Omit for no expiry (long-lived).",
    )
    args = parser.parse_args()

    invalid = [s for s in args.scopes if s not in VALID_SCOPES]
    if invalid:
        print(f"Error: invalid scopes {invalid}. Valid: {VALID_SCOPES}", file=sys.stderr)
        sys.exit(2)

    expires_at = None
    if args.expires_days is not None:
        from datetime import datetime, timedelta
        expires_at = datetime.utcnow() + timedelta(days=args.expires_days)

    with get_db_context() as db:
        user = get_user_by_username(db, args.username)
        if not user:
            print(f"Error: user '{args.username}' not found", file=sys.stderr)
            sys.exit(1)

        full_key, prefix = generate_api_key()
        api_key = create_api_key(
            db=db,
            user_id=user.id,
            key_hash=hash_api_key(full_key),
            prefix=prefix,
            name=args.name,
            scopes=args.scopes,
            expires_at=expires_at,
        )

    print("=" * 60)
    print(f"  API key minted for user '{args.username}'")
    print("=" * 60)
    print(f"  id:      {api_key.id}")
    print(f"  name:    {api_key.name}")
    print(f"  scopes:  {api_key.scopes}")
    print(f"  expires: {api_key.expires_at.isoformat() if api_key.expires_at else 'never'}")
    print(f"  prefix:  {prefix}")
    print()
    print(f"  KEY (shown once):")
    print(f"  {full_key}")
    print("=" * 60)
    print("  Use with:  curl -H 'X-API-Key: <key>' ...")
    print("=" * 60)


if __name__ == "__main__":
    main()
