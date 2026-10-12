"""Gossip-based membership: nodes learn about each other by periodically
exchanging their member tables with random peers."""

from .gossip import Gossip, Member

__all__ = ["Gossip", "Member"]
