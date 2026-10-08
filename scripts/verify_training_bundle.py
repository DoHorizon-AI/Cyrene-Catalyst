"""Compatibility wrapper for the packaged independent bundle verifier."""

from cyrene_catalyst_consumer.training_bundle import learned_messages, main, verify_bundle

__all__ = ["learned_messages", "main", "verify_bundle"]

if __name__ == "__main__":
    main()
