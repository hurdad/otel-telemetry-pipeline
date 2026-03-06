from app import main


def test_observable_gauge_reflects_items_count() -> None:
    main.items.clear()

    main.items[1] = {"id": 1, "name": "alpha"}
    main.items[2] = {"id": 2, "name": "beta"}

    observations = main.observe_current_items(None)

    assert len(observations) == 1
    assert observations[0].value == 2
    assert observations[0].attributes["service.name"] == main.OTEL_SERVICE_NAME

    main.items.pop(2)
    observations = main.observe_current_items(None)
    assert observations[0].value == 1

    main.items.clear()
