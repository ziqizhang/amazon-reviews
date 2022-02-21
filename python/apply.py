from top2vec import Top2Vec
from sklearn.datasets import fetch_20newsgroups
import sys
import pickle

if __name__ == '__main__':

    model=Top2Vec.load(sys.argv[1])
    topics = model.get_num_topics()
    topic_words, word_scores, topic_nums = model.get_topics()
    for i in range(0, topics):
        words = topic_words[i]
        model.generate_topic_wordcloud(i)
        print()

    exit(0)
    topic_words, word_scores, topic_scores, topic_nums = model.search_topics(keywords=["medicine"], num_topics=5)
    for topic in topic_nums:
        model.generate_topic_wordcloud(topic)