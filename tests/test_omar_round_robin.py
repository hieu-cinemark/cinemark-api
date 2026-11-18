import asyncio

from app.services.omar import omar_tiktok as ot


def test_round_robin_interleaves_items():
    calls: list[str] = []
    left = {"a": 3, "b": 3}

    async def step(item):
        calls.append(item)
        left[item] -= 1
        return 0.0 if left[item] else None

    asyncio.run(ot.run_round_robin(["a", "b"], step, workers=1, should_stop=lambda: False))
    assert calls == ["a", "b", "a", "b", "a", "b"]
