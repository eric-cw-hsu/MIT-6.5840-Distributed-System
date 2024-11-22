# Lab 2

Implement a Key-Value Server that follows the Linearizability strategy.

- Use a mutex lock to prevent request race conditions.
- Use a response cache to prevent duplicate requests caused by network errors.