'''
python3 Scripts/NewsDataFetcher.py -g (or)
python3 Scripts/NewsDataFetcher.py -n sample.csv    
'''

import NewsAPICallScripts.NewsData as NewsData
import NewsAPICallScripts.NewsCatcherData as NewsCatcherData
import NewsAPICallScripts.NYTData as NYTData

import sys, getopt

if __name__ == "__main__":
    filename = "NewsData.csv"
    
    argList = sys.argv[1:]
    
    options = "gf:n:o"
    long_options = ["global", "file =", "new_file =", "old_file"]
    
    try:
        arguments, values = getopt.getopt(argList, options, long_options)
        
        if(len(argList) == 0):
            NewsData.main(filename);
            NewsCatcherData.main(filename);
            NYTData.main(filename);
        else:
            for curArg, curVal in arguments:
                if(curArg == "-g" or curArg == "--global"):
                    filename = "NewsData.csv"
                elif(curArg == "-o" or curArg == "--old_file"):
                    filename = "NewsData.csv"
                elif(curArg == "-f" or curArg == "--file"):
                    filename = curVal
                elif(curArg == "-n" or curArg == "--new_file"):
                    filename = curVal
                
                # print(curArg, curVal)
                NewsData.main(filename);
                NewsCatcherData.main(filename);
                NYTData.main(filename);
            pass
    except getopt.error as err:
        # output error, and return with an error code
        print (str(err))
    