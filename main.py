import xml.sax
import mwparserfromhell
import logging
import concurrent.futures

logging.basicConfig(filename="wiki_dump_handler.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class WikiDumpHandler(xml.sax.ContentHandler):
    def __init__(self):
        self.current_tag = ""
        self.title = ""
        self.text = ""
        self.articles = []

    def startElement(self, tag, attributes):
        self.current_tag = tag

    def endElement(self, tag):
        if tag == "page":
            try:
                self.articles.append((self.title, self.clean_text()))
            except Exception as e:
                logging.error(f"Error cleaning text for title '{self.title}': {e}")

            self.title = ""
            self.text = ""

        self.current_tag = ""

    def characters(self, content):
        if self.current_tag == "title":
            self.title += content
        elif self.current_tag == "text":
            self.text += content

    def clean_text(self):
        parsed_text = mwparserfromhell.parse(self.text)
        clean_text = parsed_text.strip_code()
        return clean_text

def write_articles_to_file(articles):
    with open("clean_wikipedia.txt", "a", encoding="utf-8") as outfile:
        for title, clean_text in articles:
            outfile.write(f"=== {title} ===\n")
            outfile.write(clean_text + "\n\n")

def main():
    dump_file = "trwiki-latest-pages-articles.xml"

    parser = xml.sax.make_parser()
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)
    handler = WikiDumpHandler()
    parser.setContentHandler(handler)

    try:
        with open(dump_file, "r", encoding="utf-8") as infile:
            parser.parse(infile)
    except Exception as e:
        logging.error(f"Error parsing the file: {e}")
        return

    articles = handler.articles
    chunk_size = 100
    articles_chunks = [articles[i:i + chunk_size] for i in range(0, len(articles), chunk_size)]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(write_articles_to_file, articles_chunks)

    logging.info("Processing completed successfully.")

if __name__ == "__main__":
    main()
