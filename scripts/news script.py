import os
from bs4 import BeautifulSoup
import getpass
import pandas as pd
import json

username = getpass.getuser()
data_dir = f"/Users/{username}/Downloads/reuters21578"
all_articles = []

print("Script started")
print(f"Data directory: {data_dir}")

if not os.path.exists(data_dir):
    print(f"Data directory does not exist: {data_dir}")
else:
    for file in os.listdir(data_dir):
        if file.endswith(".sgm"):
            filepath = os.path.join(data_dir, file)
            with open(filepath, "r", encoding="latin-1") as f:
                data = f.read()
                soup = BeautifulSoup(data, "html.parser")
                for reuters in soup.find_all("reuters"):
                    text = reuters.body.get_text() if reuters.body else ""
                    title = reuters.title.get_text() if reuters.title else ""
                    all_articles.append({"title": title, "text": text})

    print(f"Total articles collected: {len(all_articles)}")
    if all_articles:
        print("First article (JSON preview):")
        print(json.dumps(all_articles[0], indent=4))

    df = pd.DataFrame(all_articles)
    print(f"DataFrame shape: {df.shape}")
    print("\nDataFrame preview:")
    print(df[['title', 'text']].head(5).to_string(index=False))
    # Save to CSV in the current project directory
    output_path = "/Users/gavinxiang/Desktop/Group-3-Final/reuters21578_clean.csv"
    df.to_csv(output_path, index=False)
    print(f"DataFrame saved to {output_path}")
