async def split_list(input_list, chunk_size=500):
    if isinstance(input_list, set):
        input_list = list(input_list)
    return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]

def array_to_string(arr):
 
    if not isinstance(arr, list):
        raise ValueError("Input must be a list.")
    return ",".join(map(str, arr))

async def remove_duplicates(models):

    seen = set()
    unique_models = []
    for model in models:
        if model.unique_key not in seen:
            seen.add(model.unique_key)
            unique_models.append(model)
    return unique_models

async def process_and_remove_duplicates(models, additional_list):

    seen = set(additional_list)
    unique_keys_in_additional_list = set(additional_list)
    processed_models = []

    for model in models:
        if model.unique_key not in seen:
            processed_models.append(model)

    return processed_models