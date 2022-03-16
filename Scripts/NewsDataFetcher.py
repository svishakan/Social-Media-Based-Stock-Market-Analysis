import NewsAPICallScripts.NewsData as NewsData
import NewsAPICallScripts.NewsCatcherData as NewsCatcherData
import NewsAPICallScripts.NYTData as NYTData

if __name__ == "__main__":
    filename = "NewsData.csv"
    
    NewsData.main(filename);
    NewsCatcherData.main(filename);
    NYTData.main(filename);