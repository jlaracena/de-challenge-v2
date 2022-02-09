# DE Challenge

This is the New Challenge for Data Engineers.

The EPL (English Premier League) has information of seasons between 09-10 and 18-19. They hire you as a DE to process the data per season to get some information relevant for the EPL and also send it to the teams and media involved.

For this Challenge: 
- We want you to do a Job that gives us the Data for the Analytics team, but with a few concerns:
    - The Job must be an ETL code in Java or Scala or Python.
    - We want a Deployment for this code.
- We want you to propose an arquitecture for the case below.
- We want a deployment for the ETL, please remember that best deployments are the easier to execute.


# Architecture Case
We want an architecture proposition for this case:

The EPL give different awards each season to the players and teams who played, the awards are:
- Best Scoring Team
- Best Coach
- Best Scoring Player
- Less Beaten Goalkepper
- Best Assist Player
- Player who traveled the most kilometers per game played
- Golden Boy (Best Sub20 Player)
- Others to be defined each season

During each match, the EPL records information of each of the teams involved (Roster of players with name and last name, number, name of the coach), the goals in favor and against before the match. Also, during the course of the match it records in real time the shots, shots on goal, goals, assists, fouls, cards and goalkeeper saves.

Each player has a device that allows to record the distance covered during the minutes he was on the field, lost water and minutes played. This information is sent in real time to a central server (provided by the owners of the device) and sent to each of the analysis centers of each team for future analysis.

The information of the match and players stats are crossed and sent to different sports sites in real time, in order to have the most recent information of each ongoing match, as well as to the bet houses to change every 5 minutes the % of bet on each of the alternatives they offer. 

Propose an architecture for the given situation, preferably with open source tools. And save in the architecture case folder (image in png or jpg)

# ETL Job

The job must receive the datasets & brings a few things:

- The position table for all the seasons.
- **The other requeriments will be in the email**:
    - Equipo con la mejor relación de disparos al arco terminando en gol por temporada.
    - Equipo más goleado por temporada.

The data is in the folder data/ in the root. The report needs to be exported to a file, you can chose the extension of the file, but remember this is an ETL Job.

Please remember to save your code in src folder.


## Acronyms Meaning

Name | Meaning
--- | --- 
DIV | division (E0, premier league)  
Date | date   
HOMETEAM | Equipo Local   
AWAYTEAM | Equipo Visita   
FTHG | Full Time Home Goals  
FTAG | Full Time Away Goals  
FTR | Full Time Result (H, A, D)[Home, Away, Draw] 
HTHG | Half Time Home Goals  
HTAG | Half Time Away Goals  
HTR | Half Time Result  
Referee | Arbitro   
HS | Home Shots   
AS | Away Shots   
HST | Home Shots Target  
AST | Away Shots Target  
HF | Home Fault   
AF | Away Faut   
HC | Home Card   
AC | Away Card   
HY | Home Yellow   
AY | Away Yellow   
HR | Home Red   
AR | Away Red

# Deployment
We want you to tell us how to deploy your job and run it in any environment, so please provide very clear and detailed instructions. If you use any cloud resources, remember to give public access and ask for the reviewers mails to deploy the solution. In any case we preffer open source solutions.

Plase save the deployment in the deploy folder.

# Concerns
- You can create a new README.md (choose a new name) for anything you want to tell us. Please don't name it README.md
- We want to see if you know how to code in a professional way, so use the best practices of Software Engineering!
- This is an ETL Job, so show us all you know about good practices to do ETL's.
- Save all the changes in your personal GitHub account using a Fork from this repository and send us the link to clone and see your repository.

## Disclaimer Note
``` This challenge is your cover letter, the elections you choose to do & not to do matters!!, and will be asked in the next interview. If you don't finish the challenge, we will have to end your application```

## Datasets
Dataset for the challenge is inside folder "data"
