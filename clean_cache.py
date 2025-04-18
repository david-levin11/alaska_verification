import os
import shutil

def clean_herbie_cache(base_path="~/data", models=["nbm", "urma_ak", "rtma_ak"]):
    base_path = os.path.expanduser(base_path)
    deleted_dirs = []

    for model in models:
        model_path = os.path.join(base_path, model)
        if os.path.exists(model_path):
            for subdir in os.listdir(model_path):
                full_path = os.path.join(model_path, subdir)
                if os.path.isdir(full_path):
                    print(f"🧹 Deleting {full_path}")
                    shutil.rmtree(full_path)
                    deleted_dirs.append(full_path)
        else:
            print(f"⚠️ Path not found: {model_path}")

    print(f"\n✅ Deleted {len(deleted_dirs)} cache folders.")
    return deleted_dirs

if __name__ == "__main__":
    clean_herbie_cache()
