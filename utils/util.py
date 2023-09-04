from elasticsearch import Elasticsearch


# Helper function to index data in Elasticsearch.
def index_data(records, index):
    for record in records:
        yield {
            "_index": index,
            "_source": record
        }


def search_books(es: Elasticsearch, term: str, user_id: int):
    should_clauses = [
        {"match": {"isbn": term}},
        {"match": {"book_title": term}},
        {"match": {"book_author": term}},
        {"match": {"summary": term}},
        {"match": {"publisher": term}},
        {"match": {"category": term}}
    ]

    # Only add year_of_publication match clause if term is numeric.
    if term.isnumeric():
        should_clauses.append({"match": {"year_of_publication": term}})

    # 1. Search for books matching the term.
    res = es.search(index="books", query={
        "bool": {
            "should": should_clauses
        }
    })

    scored_books = []

    # 2. For each book, check if there's a user rating.
    for hit in res['hits']['hits']:
        isbn = hit["_source"]["isbn"]
        es_score = hit["_score"]

        # Fetch user's rating from book_ratings index.
        rating_res = es.search(index="ratings", query={
            "bool": {
                "must": [
                    {"match": {"uid": user_id}},
                    {"match": {"isbn": isbn}}
                ]
            }
        })

        user_rating = 0
        if rating_res["hits"]["hits"]:
            user_rating = rating_res["hits"]["hits"][0]["_source"]["rating"]

        # Simple combination of Elasticsearch score and user rating.
        combined_score = es_score + 2 * user_rating

        scored_books.append((hit["_source"], combined_score))

    # 3. Sort by combined score.
    scored_books.sort(key=lambda x: x[1], reverse=True)

    # 4. Return top 10%.
    top_10_percent = int(0.1 * len(scored_books))
    return scored_books[:top_10_percent]

#%%
