def summarize_pokemon(name: str, poke_id: int) -> dict:
    record = {
        "id": poke_id,
        "name": name,
        "abilities": [],
        "types": []
    }

    return record

rec = summarize_pokemon("bulbasaur", 1)
print(rec)
print(type(rec))