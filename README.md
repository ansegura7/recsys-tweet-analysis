# RecSys 2022 Twitter Interaction Analysis
Analysis of the interaction between Twitter users during the <a href="https://recsys.acm.org/recsys22/" target="_blank">ACM RecSys 2022</a> conference.

![WordCloud](https://raw.githubusercontent.com/ansegura7/recsys-tweet-analysis/master/img/wordcloud.png)

## Run Tweepy bot
Commands to create the virtual environment (`.venv`) and run the **Tweepy bot** that downloads the list of tweets:

```console
  cd C:\Dev Projects\recsys-tweet-analysis
  python -m venv .venv
  .venv\Scripts\activate
  python code\tweets_downloader.py
  deactivate
```

**Note**: The following command should be executed only the first time:

```console
  python -m venv .venv
```

## Project Dependencies
The list of project requirements can be found in the following text <a href="https://github.com/ansegura7/recsys-tweet-analysis/blob/main/requirements.txt">file</a>.

To automatically install the same version used for all dependencies, run the following commands in the terminal.

```console
  cd C:\Dev Projects\recsys-tweet-analysis
  .venv\Scripts\activate
  pip install -r requirements.txt
```

To manually install the latest version of Tweepy, run the following commands in the terminal:

```console
  pip install tweepy
```

## Performed Analysis
1. <a href="https://ansegura7.github.io/recsys-tweet-analysis/analysis/AccountAnalytics.html" >Twitter Interaction Analysis</a>
2. <a href="https://observablehq.com/@ansegura7/force-directed-graph">Network Analysis</a>

## Disclaimer
Neither the Twitter accounts nor the content of the tweets are used for profit. Only general statistics are calculated about them and shared with the RecSys community.

## Contributing and Feedback
Any kind of feedback/suggestions would be greatly appreciated (algorithm design, documentation, improvement ideas, spelling mistakes, etc...). If you want to make a contribution to the course you can do it through a PR.

## Author
- Created by Andrés Segura-Tinoco
- Updated on Sep 26, 2022

## License
This project is licensed under the terms of the <a href="https://github.com/ansegura7/recsys-tweet-analysis/blob/main/LICENSE">MIT license</a>.
