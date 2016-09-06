###############################################
# Team : 4 (JayaChandra , Mihir and Nilesh )
###############################################

library(sqldf)
library(data.table)

############################### March ############################################

# Loaading Tagwise badges
BandroidMar1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Mar_android 1-15.csv")
BandroidMar2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Mar_android 16-31.csv")
BCsharpMar1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Mar_c# 1-15.csv")
BCsharpMar2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Mar_c# 16-31.csv")
BjavaMar1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Mar_Java 1-15.csv")
BjavaMar2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Mar_Java 16-31.csv")
BjavascriptMar1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Mar_javascript 1-15.csv")
BjavascriptMar2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Mar_javascript 16-31.csv")
BphpMar1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Mar_php 1-15.csv")
BphpMar2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Mar_php 16-31.csv")

# loading tagwise user reputation for top 5 tags
RandroidMar1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Mar_android 1-15.csv")
RandroidMar2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Mar_android 16-31.csv")
RCsharpMar1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Mar_c# 1-15.csv")
RCsharpMar2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Mar_c# 16-31.csv")
RjavaMar1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Mar_Java 1-15.csv")
RjavaMar2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Mar_Java 16-31.csv")
RjavascriptMar1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Mar_Javascript 1-15.csv")
RjavascriptMar2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Mar_Javascript 16-31.csv")
RphpMar1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Mar_php 1-15.csv")
RphpMar2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Mar_php 16-31.csv")

# Loading calculated total reputation of user at that point
Rrep11 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\ARep_Mar 1-15.csv")
Rrep21 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\ARep_Mar 16-31.csv")

#joining all tables
Rrep1 = sqldf("select a.UserId,a.AReputation , bc.GoldBadge as csharp_gold,
              bc.SilverBadge as csharp_silver,bc.BronzeBadge as csharp_bronze,
              ba.GoldBadge as android_gold,ba.SilverBadge as android_silver,ba.BronzeBadge as android_bronze,
              bj.GoldBadge as java_gold,bj.SilverBadge as java_silver,bj.BronzeBadge as java_bronze,
              bjs.GoldBadge as javascript_gold,bjs.SilverBadge as javascript_silver,bjs.BronzeBadge as javascript_bronze,
              bp.GoldBadge as php_gold,bp.SilverBadge as php_silver,bp.BronzeBadge as php_bronze,
Ra.AReputation as Rep_android,Rc.AReputation as Rep_csharp,Rj.AReputation as Rep_java,
Rjs.AReputation as Rep_javascript, Rp.AReputation as Rep_php
              from Rrep11 as a left join BCsharpMar1  as bc on a.UserId = bc.UserID  left join
              BandroidMar1 as  ba on   a.UserId = ba.UserID  left join BjavaMar1 as  bj 
              on a.UserId = bj.UserID left join  BjavascriptMar1 as bjs on a.UserId = bjs.UserID
              left join  BphpMar1 as bp on a.UserId = bp.UserID left join RandroidMar1 as Ra on a.UserId = Ra.UserId
left join RCsharpMar1 as Rc on a.UserId = Rc.UserId left join RjavaMar1 as Rj on a.UserId = Rj.UserId 
left join RjavascriptMar1 as Rjs on a.UserId = Rjs.UserId left join RphpMar1 as Rp on a.UserId = Rp.UserId ")


Rrep2 = sqldf("select a.UserId,a.AReputation , bc.GoldBadge as csharp_gold,
              bc.SilverBadge as csharp_silver,bc.BronzeBadge as csharp_bronze,
              ba.GoldBadge as android_gold,ba.SilverBadge as android_silver,ba.BronzeBadge as android_bronze,
              bj.GoldBadge as java_gold,bj.SilverBadge as java_silver,bj.BronzeBadge as java_bronze,
              bjs.GoldBadge as javascript_gold,bjs.SilverBadge as javascript_silver,bjs.BronzeBadge as javascript_bronze,
              bp.GoldBadge as php_gold,bp.SilverBadge as php_silver,bp.BronzeBadge as php_bronze,
Ra.AReputation as Rep_android,Rc.AReputation as Rep_csharp,Rj.AReputation as Rep_java,
Rjs.AReputation as Rep_javascript, Rp.AReputation as Rep_php
              from Rrep21 as a left join BCsharpMar2  as bc on a.UserId = bc.UserID  left join
              BandroidMar2 as  ba on   a.UserId = ba.UserID  left join BjavaMar2 as  bj 
              on a.UserId = bj.UserID left join  BjavascriptMar2 as bjs on a.UserId = bjs.UserID
              left join  BphpMar2 as bp on a.UserId = bp.UserID left join RandroidMar2 as Ra on a.UserId = Ra.UserId
left join RCsharpMar2 as Rc on a.UserId = Rc.UserId left join RjavaMar2 as Rj on a.UserId = Rj.UserId 
left join RjavascriptMar2 as Rjs on a.UserId = Rjs.UserId left join RphpMar2 as Rp on a.UserId = Rp.UserId")

# loading answers
Rans1 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AMar 1-5.csv")
Rans2 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AMar 5-10.csv")
Rans3 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AMar 10-15.csv")
Rans4 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AMar 15-20.csv")
Rans5 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AMar 20-25.csv")
Rans6 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AMar 25-31.csv")

# combing answers
Ran1_15 = rbind(Rans1,Rans2,Rans3)
Ran16_31 = rbind(Rans4,Rans5,Rans6)

# Merge answers with answer reputation frame 
Rans1_15 = merge(x=Ran1_15,y=Rrep1,by.x="AnsUserID",by.y="UserId",all.x = T)
Rans16_31 = merge(x=Ran16_31,y=Rrep2,by.x="AnsUserID",by.y="UserId",all.x = T)

# Replace NA with zero 
Rans1_15[is.na(Rans1_15)] <- 0
Rans16_31[is.na(Rans16_31)] <- 0

# Loading calculated reputation of users
Rqrep1 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\QRep_Mar 1-15.csv")
Rqrep2 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\QRep_Mar 16-31.csv")

# loading Questions 
Rque1_15 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\QMar 1-15.csv")
Rque16_31 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\QMar 15-31.csv")

# Merge questions with question reputation 
Rques1_15 = merge(x=Rque1_15,y=Rqrep1,by.x = "QueOwnerUserID",by.y = "UserId",all.x = T )
Rques16_31 = merge(x=Rque16_31,y=Rqrep2,by.x = "QueOwnerUserID",by.y = "UserId",all.x = T )

# Replace NA with zero 

Rques1_15[is.na(Rques1_15)] <- 0
Rques16_31[is.na(Rques16_31)] <- 0

# loading Answer votes
RansVote1 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar1.csv")
RansVote2 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar2.csv")
RansVote3 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar3.csv")
RansVote4 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar4.csv")
RansVote5 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar5.csv")
RansVote6 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar6.csv")
RansVote7 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar7.csv")
RansVote8 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar8.csv")
RansVote9 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar9.csv")
RansVote10 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar10.csv")

RansVote11 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar11.csv")
RansVote12 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar12.csv")
RansVote13 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar13.csv")
RansVote14 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar14.csv")
RansVote15 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar15.csv")
RansVote16 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar16.csv")
RansVote17 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar17.csv")
RansVote18 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar18.csv")
RansVote19 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar19.csv")
RansVote20 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar20.csv")

RansVote21 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar21.csv")
RansVote22 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar22.csv")
RansVote23 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar23.csv")
RansVote24 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar24.csv")
RansVote25 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar25.csv")
RansVote26 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar26.csv")
RansVote27 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar27.csv")
RansVote28 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar28.csv")
RansVote29 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar29.csv")
RansVote30 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar30.csv")
RansVote31 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMar31.csv")

#Merge all AnsVotes 
RallAnsVotes = rbind(RansVote1,RansVote2,RansVote3,RansVote4,RansVote5,RansVote6,RansVote7,RansVote8,RansVote9,RansVote10,
                    RansVote11,RansVote12,RansVote13,RansVote14,RansVote15,RansVote16,RansVote17,RansVote18,RansVote19,RansVote20,
                    RansVote21,RansVote22,RansVote23,RansVote24,RansVote25,RansVote26,RansVote27,RansVote28,RansVote29,RansVote30,RansVote31)

#Merge all questions 
RallQues = rbind(Rques1_15,Rques16_31)

#Merge all answers 
RallAns = rbind(Rans1_15,Rans16_31)

# Join questions and answers 
RAllPosts = merge(x=RallQues[,c("QueID", "QueAcceptedAnswerId","QueCreationDate","QueOwnerReputation",
                                "QueTags","QReputation","QueviewCount","QueOwnerUserID")],
                  y=RallAns[,c("AnsUserID","AnsID", "AnsParentId","AnsCreationdate","AnsReputation","AReputation","AnsCommnetCount","AnsBody",
                               "csharp_gold","csharp_silver","csharp_bronze","android_gold","android_silver","android_bronze",   
                               "java_gold","java_silver","java_bronze","javascript_gold","javascript_silver","javascript_bronze",
                               "php_gold","php_silver","php_bronze","Rep_android","Rep_csharp","Rep_java","Rep_javascript","Rep_php")],
                  by.x="QueID",by.y="AnsParentId" )


#Join Allposts data with votes data
Ralldata = merge(x=RAllPosts , y = RallAnsVotes, by.x= "AnsID", by.y ="AnsID",all.x = T)

# ordering data based on queID and voteID
Ralldata= Ralldata[order(Ralldata$QueID), ]

# deleting extra column 
Ralldata$AnsVoteID = NULL 

################################### April ##########################################

# Loaading Tagwise badges
BandroidApr1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Apr_android 1-15.csv")
BandroidApr2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Apr_android 16-30.csv")
BCsharpApr1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Apr_c# 1-15.csv")
BCsharpApr2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Apr_c# 16-30.csv")
BjavaApr1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Apr_Java 1-15.csv")
BjavaApr2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Apr_Java 16-30.csv")
BjavascriptApr1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Apr_javascript 1-15.csv")
BjavascriptApr2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Apr_javascript 16-30.csv")
BphpApr1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Apr_php 1-15.csv")
BphpApr2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_Apr_php 16-30.csv")

# loading tagwise user reputation for top 5 tags
RandroidApr1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Apr_android 1-15.csv")
RandroidApr2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Apr_android 16-30.csv")
RCsharpApr1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Apr_C# 1-15.csv")
RCsharpApr2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Apr_C# 16-30.csv")
RjavaApr1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Apr_Java 1-15.csv")
RjavaApr2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Apr_Java 16-30.csv")
RjavascriptApr1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Apr_javascript 1-15.csv")
RjavascriptApr2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Apr_javascript 16-30.csv")
RphpApr1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Apr_php 1-15.csv")
RphpApr2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_Apr_php 16-30.csv")

# Loading calculated total reputation of user at that point
Arep11 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\ARep_Apr 1-15.csv")
Arep21 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\ARep_Apr 16-30.csv")

#joining all tables
Arep1 = sqldf("select a.UserId,a.AReputation , bc.GoldBadge as csharp_gold,
              bc.SilverBadge as csharp_silver,bc.BronzeBadge as csharp_bronze,
              ba.GoldBadge as android_gold,ba.SilverBadge as android_silver,ba.BronzeBadge as android_bronze,
              bj.GoldBadge as java_gold,bj.SilverBadge as java_silver,bj.BronzeBadge as java_bronze,
              bjs.GoldBadge as javascript_gold,bjs.SilverBadge as javascript_silver,bjs.BronzeBadge as javascript_bronze,
              bp.GoldBadge as php_gold,bp.SilverBadge as php_silver,bp.BronzeBadge as php_bronze,
Ra.AReputation as Rep_android,Rc.AReputation as Rep_csharp,Rj.AReputation as Rep_java,
Rjs.AReputation as Rep_javascript, Rp.AReputation as Rep_php
              from Arep11 as a left join BCsharpApr1  as bc on a.UserId = bc.UserID  left join
              BandroidApr1 as  ba on   a.UserId = ba.UserID  left join BjavaApr1 as  bj 
              on a.UserId = bj.UserID left join  BjavascriptApr1 as bjs on a.UserId = bjs.UserID
              left join  BphpApr1 as bp on a.UserId = bp.UserID  left join RandroidApr1 as Ra on a.UserId = Ra.UserId
left join RCsharpApr1 as Rc on a.UserId = Rc.UserId left join RjavaApr1 as Rj on a.UserId = Rj.UserId 
left join RjavascriptApr1 as Rjs on a.UserId = Rjs.UserId left join RphpApr1 as Rp on a.UserId = Rp.UserId ")


Arep2 = sqldf("select a.UserId,a.AReputation , bc.GoldBadge as csharp_gold,
              bc.SilverBadge as csharp_silver,bc.BronzeBadge as csharp_bronze,
              ba.GoldBadge as android_gold,ba.SilverBadge as android_silver,ba.BronzeBadge as android_bronze,
              bj.GoldBadge as java_gold,bj.SilverBadge as java_silver,bj.BronzeBadge as java_bronze,
              bjs.GoldBadge as javascript_gold,bjs.SilverBadge as javascript_silver,bjs.BronzeBadge as javascript_bronze,
              bp.GoldBadge as php_gold,bp.SilverBadge as php_silver,bp.BronzeBadge as php_bronze,
Ra.AReputation as Rep_android,Rc.AReputation as Rep_csharp,Rj.AReputation as Rep_java,
Rjs.AReputation as Rep_javascript, Rp.AReputation as Rep_php
              from Arep21 as a left join BCsharpApr2  as bc on a.UserId = bc.UserID  left join
              BandroidApr2 as  ba on   a.UserId = ba.UserID  left join BjavaApr2 as  bj 
              on a.UserId = bj.UserID left join  BjavascriptApr2 as bjs on a.UserId = bjs.UserID
              left join  BphpApr2 as bp on a.UserId = bp.UserID left join RandroidApr2 as Ra on a.UserId = Ra.UserId
left join RCsharpApr2 as Rc on a.UserId = Rc.UserId left join RjavaApr2 as Rj on a.UserId = Rj.UserId 
left join RjavascriptApr2 as Rjs on a.UserId = Rjs.UserId left join RphpApr2 as Rp on a.UserId = Rp.UserId")

# loading answers
Aans1 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\Aapr 1-5.csv")
Aans2 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\Aapr 6-10.csv")
Aans3 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\Aapr 11-15.csv")
Aans4 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\Aapr 16-20.csv")
Aans5 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\Aapr 21-25.csv")
Aans6 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\Aapr 25-30.csv")

# combing answers
Aan1_15 = rbind(Aans1,Aans2,Aans3)
Aan16_31 = rbind(Aans4,Aans5,Aans6)

# Merge answers with answer reputation frame 
Aans1_15 = merge(x=Aan1_15,y=Arep1,by.x="AnsUserID",by.y="UserId",all.x = T)
Aans16_31 = merge(x=Aan16_31,y=Arep2,by.x="AnsUserID",by.y="UserId",all.x = T)

# Replace NA with zero 
Aans1_15[is.na(Aans1_15)] <- 0
Aans16_31[is.na(Aans16_31)] <- 0

# Loading calculated reputation of users
Aqrep1 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\QRep_Apr 1-15.csv")
Aqrep2 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\QRep_Apr 16-30.csv")

# loading Questions 
Aque1_15 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\Qapr 1-15.csv")
Aque16_31 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\Qapr 16-30.csv")

# Merge questions with question reputation 
Aques1_15 = merge(x=Aque1_15,y=Aqrep1,by.x = "QueOwnerUserID",by.y = "UserId",all.x = T )
Aques16_31 = merge(x=Aque16_31,y=Aqrep2,by.x = "QueOwnerUserID",by.y = "UserId",all.x = T )

# Replace NA with zero 
Aques1_15[is.na(Aques1_15)] <- 0
Aques16_31[is.na(Aques16_31)] <- 0

# loading Answer votes
AansVote1 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteApr1-2.csv")
AansVote2 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteApr3-4.csv")
AansVote3 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteApr5-6.csv")
AansVote4 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteApr7-8.csv")
AansVote5 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteApr9-10.csv")
AansVote6 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteApr11-12.csv")
AansVote7 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteApr13-14.csv")
AansVote8 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteApr15-16.csv")
AansVote9 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteApr17-18.csv")
AansVote10 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteApr19-20.csv")

AansVote11 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteApr21-22.csv")
AansVote12 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteApr23-24.csv")
AansVote13 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteApr25-26.csv")
AansVote14 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteApr27-28.csv")
AansVote15 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteApr29-30.csv")

#Merge all AnsVotes 
AallAnsVotes = rbind(AansVote1,AansVote2,AansVote3,AansVote4,AansVote5,AansVote6,AansVote7,
                    AansVote8,AansVote9,AansVote10,
                    AansVote11,AansVote12,AansVote13,AansVote14,AansVote15)

#Merge all questions 
AallQues = rbind(Aques1_15,Aques16_31)

#Merge all answers 
AallAns = rbind(Aans1_15,Aans16_31)


AAllPosts = merge(x=AallQues[,c("QueID", "QueAcceptedAnswerId","QueCreationDate","QueOwnerReputation",
                               "QueTags","QReputation","QueviewCount","QueOwnerUserID")],
                 y=AallAns[,c("AnsUserID","AnsID", "AnsParentId","AnsCreationdate","AnsReputation","AReputation","AnsCommnetCount","AnsBody",
                             "csharp_gold","csharp_silver","csharp_bronze","android_gold","android_silver","android_bronze",   
                            "java_gold","java_silver","java_bronze","javascript_gold","javascript_silver","javascript_bronze",
                            "php_gold","php_silver","php_bronze","Rep_android","Rep_csharp","Rep_java","Rep_javascript","Rep_php")],
                 by.x="QueID",by.y="AnsParentId" )


#Join Allposts data with votes data
Aalldata = merge(x=AAllPosts , y = AallAnsVotes, by.x= "AnsID", by.y ="AnsID",all.x = T)


# ordering data based on queID and voteID
Aalldata= Aalldata[order(Aalldata$QueID), ]


##################################### May #################################

# Loaading Tagwise badges
BandroidMay1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_May_android 1-15.csv")
BandroidMay2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_May_android 16-31.csv")
BCsharpMay1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_May_c# 1-15.csv")
BCsharpMay2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_May_c# 16-31.csv")
BjavaMay1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_May_java 1-15.csv")
BjavaMay2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_May_java 16-31.csv")
BjavascriptMay1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_May_javascript 1-15.csv")
BjavascriptMay2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_May_javascript 16-31.csv")
BphpMay1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_May_php 1-15.csv")
BphpMay2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ABadge_May_php 16-31.csv")

# loading tagwise user reputation for top 5 tags
RandroidMay1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_May_android 1-15.csv")
RandroidMay2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_May_android 16-31.csv")
RCsharpMay1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_May_C# 1-15.csv")
RCsharpMay2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_May_C# 16-31.csv")
RjavaMay1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_May_Java 1-15.csv")
RjavaMay2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_May_java 16-31.csv")
RjavascriptMay1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_May_javascript 1-15.csv")
RjavascriptMay2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_May_javascript 16-31.csv")
RphpMay1 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_May_php 1-15.csv")
RphpMay2 = read.csv("D:\\Python\\Project\\NIlesh\\Final\\Data\\ARep_May_php 16-31.csv")

# Loading calculated total reputation of user at that point
Mrep11 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\ARep_May 1-15.csv")
Mrep21 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\ARep_May 16-31.csv")

#joining all tables
Mrep1 = sqldf("select a.UserId,a.AReputation , bc.GoldBadge as csharp_gold,
              bc.SilverBadge as csharp_silver,bc.BronzeBadge as csharp_bronze,
              ba.GoldBadge as android_gold,ba.SilverBadge as android_silver,ba.BronzeBadge as android_bronze,
              bj.GoldBadge as java_gold,bj.SilverBadge as java_silver,bj.BronzeBadge as java_bronze,
              bjs.GoldBadge as javascript_gold,bjs.SilverBadge as javascript_silver,bjs.BronzeBadge as javascript_bronze,
              bp.GoldBadge as php_gold,bp.SilverBadge as php_silver,bp.BronzeBadge as php_bronze,
Ra.AReputation as Rep_android,Rc.AReputation as Rep_csharp,Rj.AReputation as Rep_java,
Rjs.AReputation as Rep_javascript, Rp.AReputation as Rep_php
              from Mrep11 as a left join BCsharpMay1  as bc on a.UserId = bc.UserID  left join
              BandroidMay1 as  ba on   a.UserId = ba.UserID  left join BjavaMay1 as  bj 
              on a.UserId = bj.UserID left join  BjavascriptMay1 as bjs on a.UserId = bjs.UserID
              left join  BphpMay1 as bp on a.UserId = bp.UserID left join RandroidMay1 as Ra on a.UserId = Ra.UserId
left join RCsharpMay1 as Rc on a.UserId = Rc.UserId left join RjavaMay1 as Rj on a.UserId = Rj.UserId 
left join RjavascriptMay1 as Rjs on a.UserId = Rjs.UserId left join RphpMay1 as Rp on a.UserId = Rp.UserId")

Mrep2 = sqldf("select a.UserId,a.AReputation , bc.GoldBadge as csharp_gold,
              bc.SilverBadge as csharp_silver,bc.BronzeBadge as csharp_bronze,
              ba.GoldBadge as android_gold,ba.SilverBadge as android_silver,ba.BronzeBadge as android_bronze,
              bj.GoldBadge as java_gold,bj.SilverBadge as java_silver,bj.BronzeBadge as java_bronze,
              bjs.GoldBadge as javascript_gold,bjs.SilverBadge as javascript_silver,bjs.BronzeBadge as javascript_bronze,
              bp.GoldBadge as php_gold,bp.SilverBadge as php_silver,bp.BronzeBadge as php_bronze,
Ra.AReputation as Rep_android,Rc.AReputation as Rep_csharp,Rj.AReputation as Rep_java,
Rjs.AReputation as Rep_javascript, Rp.AReputation as Rep_php
              from Mrep21 as a left join BCsharpMay2  as bc on a.UserId = bc.UserID  left join
              BandroidMay2 as  ba on   a.UserId = ba.UserID  left join BjavaMay2 as  bj 
              on a.UserId = bj.UserID left join  BjavascriptMay2 as bjs on a.UserId = bjs.UserID
              left join  BphpMay2 as bp on a.UserId = bp.UserID left join RandroidMay2 as Ra on a.UserId = Ra.UserId
left join RCsharpMay2 as Rc on a.UserId = Rc.UserId left join RjavaMay2 as Rj on a.UserId = Rj.UserId 
left join RjavascriptMay2 as Rjs on a.UserId = Rjs.UserId left join RphpMay2 as Rp on a.UserId = Rp.UserId ")

# loading answers
Mans1 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\Amay 1-5.csv")
Mans2 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\Amay 6-10.csv")
Mans3 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\Amay 11-15.csv")
Mans4 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\Amay 16-20.csv")
Mans5 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\Amay 21-25.csv")
Mans6 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\Amay 26-31.csv")

# combing answers
Man1_15 = rbind(Mans1,Mans2,Mans3)
Man16_31 = rbind(Mans4,Mans5,Mans6)

# Merge answers with answer reputation frame 
Mans1_15 = merge(x=Man1_15,y=Mrep1,by.x="AnsUserID",by.y="UserId",all.x = T)
Mans16_31 = merge(x=Man16_31,y=Mrep2,by.x="AnsUserID",by.y="UserId",all.x = T)

# replacing NA with 0
Mans1_15[is.na(Mans1_15)] <- 0
Mans16_31[is.na(Mans16_31)] <- 0

# Loading calculated reputation of users
Mqrep1 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\QRep_May 1-15.csv")
Mqrep2 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\QRep_May 16-31.csv")

# loading Questions 
Mque1_15 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\QMay1-15.csv")
Mque16_31 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\QMay16-31.csv")

# Merge questions with question reputation 
Mques1_15 = merge(x=Mque1_15,y=Mqrep1,by.x = "QueOwnerUserID",by.y = "UserId",all.x = T )
Mques16_31 = merge(x=Mque16_31,y=Mqrep2,by.x = "QueOwnerUserID",by.y = "UserId",all.x = T )

# Replace NA with zero 
Mques1_15[is.na(Mques1_15)] <- 0
Mques16_31[is.na(Mques16_31)] <- 0

# loading Answer votes
MansVote1 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMay1-2.csv")
MansVote2 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMay3-4.csv")
MansVote3 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMay5-6.csv")
MansVote4 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMay7-8.csv")
MansVote5 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMay9-10.csv")
MansVote6 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMay11-12.csv")
MansVote7 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMay13-14.csv")
MansVote8 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMay15-16.csv")
MansVote9 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMay17-18.csv")
MansVote10 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMay19-20.csv")

MansVote11 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMay21-22.csv")
MansVote12 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMay23-24.csv")
MansVote13 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMay25-26.csv")
MansVote14 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMay27-28.csv")
MansVote15 = read.csv("D:\\Python\\Project\\NIlesh\\Week 7\\Data_Week7_3Months\\Data\\AnsVoteMay29-30.csv")

#Merge all AnsVotes 
MallAnsVotes = rbind(MansVote1,MansVote2,MansVote3,MansVote4,MansVote5,MansVote6,MansVote7,
                    MansVote8,MansVote9,MansVote10,
                    MansVote11,MansVote12,MansVote13,MansVote14,MansVote15)

#Merge all questions 
MallQues = rbind(Mques1_15,Mques16_31)

#Merge all answers 
MallAns = rbind(Mans1_15,Mans16_31)

MAllPosts = merge(x=MallQues[,c("QueID", "QueAcceptedAnswerId","QueCreationDate","QueOwnerReputation",
                                "QueTags","QReputation","QueviewCount","QueOwnerUserID")],
                  y=MallAns[,c("AnsUserID","AnsID", "AnsParentId","AnsCreationdate","AnsReputation","AReputation","AnsCommnetCount","AnsBody",
                               "csharp_gold","csharp_silver","csharp_bronze","android_gold","android_silver","android_bronze",   
                               "java_gold","java_silver","java_bronze","javascript_gold","javascript_silver","javascript_bronze",
                               "php_gold","php_silver","php_bronze","Rep_android","Rep_csharp","Rep_java","Rep_javascript","Rep_php")],
                  by.x="QueID",by.y="AnsParentId" )

#Join Allposts data with votes data
Malldata = merge(x=MAllPosts , y = MallAnsVotes, by.x= "AnsID", by.y ="AnsID",all.x = T)

# ordering data based on queID and voteID
Malldata= Malldata[order(Malldata$QueID), ]

########################################################################

# combining all months data
data  = rbind(Ralldata,Aalldata,Malldata)

# creating data table
data =  data.table(data)

############################# Android tags ##############################
android = data[QueTags %like% "android"]

# seleccting required data required
Atotal = sqldf("select QueID,AnsID,QueAcceptedAnswerId,QueOwnerReputation,QueOwnerUserID,AnsUserID,
              QReputation,QueviewCount,AnsCreationdate,QueCreationDate,AnsCommnetCount,
length(AnsBody) - length(replace(AnsBody, ' ', '')) + 1 ANswordcount,
              AReputation,AnsReputation,QueTags,AnsAccepted,Rep_android as TagRep,android_silver as silver,android_gold as gold,
          android_bronze as bronze,AnsUpvotecount,AnsDownvotecount,AnsBounty,VoteCreationDate,
              (case when AnsAccepted = 1 then VoteCreationDate end) as Accepteddate,length(AnsBody) as LengthofBody,
              (case when AnsBody like '%<code>%' or AnsBody like '%<a href=%'  then 1 else 0 end) as flag
              from android")

Atotal[is.na(Atotal)] <- 0

# converting to datatable 
ADT = data.table(Atotal)
ADT[ ,AfterAccepted:=cumsum(AnsAccepted), by ="QueID"]
ADT[ ,Acceptdate:=max(Accepteddate), by ="QueID"]

# creating vote days as diff b/w vote creation and acceptdate
ADT = sqldf("select *,(julianday(VoteCreationDate) - julianday(Acceptdate ))as votedays  from ADT ")

# filtering vote days
Afinal = sqldf("select QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
              QReputation,QueviewCount,AReputation,AnsReputation,QueTags,AfterAccepted ,
TagRep,silver,gold, bronze,ANswordcount,AnsCommnetCount,QueOwnerUserID,AnsUserID,
              AnsCreationdate,QueCreationDate,sum(AnsUpvotecount) as upvotes,sum(AnsDownvotecount) as downvotes,LengthofBody,flag from ADT 
              group by QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
              AnsReputation,QueTags,AfterAccepted,QReputation,QueviewCount,AReputation,AnsCreationdate,QueCreationDate,
              TagRep,silver,gold, bronze,ANswordcount,AnsCommnetCount,QueOwnerUserID,AnsUserID")

Afinalnew = sqldf("select QueID,AnsID,QueOwnerReputation,QReputation,QueviewCount,AReputation,
TagRep,silver,gold, bronze,ANswordcount,AnsCommnetCount,QueOwnerUserID,AnsUserID,
                 AnsReputation,QueTags,(case when QueacceptedanswerId=AnsID then 1 else 0 end) as IsAccepted,
                 AfterAccepted,upvotes,downvotes,LengthofBody,flag from Afinal ")

# separating before and upper votes
AAfterAcceptedVotes  = Afinalnew[Afinalnew$AfterAccepted ==1,]
ABeforeAcceptedVotes  = Afinalnew[Afinalnew$AfterAccepted ==0,]

Atest = merge(x=AAfterAcceptedVotes , y = ABeforeAcceptedVotes, 
             by.x= c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation",
                     "TagRep","silver","gold","bronze","ANswordcount","AnsCommnetCount","QueOwnerUserID","AnsUserID","LengthofBody","flag"),
             by.y =c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation",
                     "TagRep","silver","gold","bronze" ,"ANswordcount","AnsCommnetCount","QueOwnerUserID","AnsUserID","LengthofBody","flag"),
             all.x = T,all.y=T)

Atest[is.na(Atest)] <- 0

AAlldetails = sqldf("select QueID,AnsID,QueOwnerReputation,
                   AnsReputation,QueTags,IsAccepted  ,QReputation,QueviewCount,
silver,gold, bronze,ANswordcount,AnsCommnetCount,QueOwnerUserID,AnsUserID,
                   [upvotes.x] As After_upvotes,[downvotes.x]  as After_downvotes,
                   [upvotes.y]  as Before_upvotes,[downvotes.y]  as Before_downvotes,
                   [upvotes.x] - [upvotes.y]  as upvote_diff,
                   [downvotes.x]-[downvotes.y]  as downvote_diff,LengthofBody,flag
                   From  Atest")

############################### Ans Sequence ##############################################

Aseq = sqldf("select   distinct AnsID,QueID,TagRep,AReputation,
            AnsCreationdate,QueCreationDate, (julianday(AnsCreationdate) - julianday(QueCreationDate)) as timediff from Afinal")

Aseq = data.table(Aseq)

# new columns for ans sequence and rep sequence using rank function
Aseq[,Ansseq:=rank(AnsCreationdate,ties.method="first"),by=QueID]
Aseq[,Repseq:=rank(-TagRep,ties.method="first"),by=QueID]

# merging with all details 
Aalltotal =  merge(x=AAlldetails, y = Aseq,by.x =c("AnsID","QueID"),by.y = c("AnsID","QueID"),all.x = T )

Alastdf = sqldf("select QueID,AnsID,IsAccepted,QueOwnerReputation,
               QReputation,AnsReputation, AReputation,QueviewCount,
TagRep,silver,gold, bronze,ANswordcount,AnsCommnetCount,QueOwnerUserID,AnsUserID,
               After_upvotes,After_downvotes,Before_upvotes,(Before_upvotes+ After_upvotes) as totalupvotes,
               (After_downvotes+Before_downvotes) as totaldownvotes,
               Before_downvotes,upvote_diff,downvote_diff,timediff,Ansseq,Repseq,LengthofBody,flag
               from Aalltotal  order by QueID ,AnsID")

Alastdf = data.table(Alastdf)
Alastdf[ ,Has_Accepted_Answer:=max(IsAccepted), by ="QueID"]
Alastdf[ ,Answer_count :=length(unique(AnsID)), by ="QueID"]
Alastdf[ ,Tag_name := "Android" ]

Alastdf = sqldf("select*, (case when IsAccepted=1 and Repseq=1 then 1 else 0 end) as Repseqflag,
               (case when IsAccepted=1 and Ansseq=1 then 1 else 0 end) as Ansseqflag ,
                 (case when IsAccepted=1 and Repseq=1 and Ansseq=1 then 1 else 0 end) as Rep_Ans_flag from Alastdf ")

Alastdf = data.table(Alastdf)
Alastdf[ ,Repseq_flag:=max(Repseqflag), by ="QueID"]
Alastdf[ ,Ansseq_flag:=max(Ansseqflag), by ="QueID"]
Alastdf[ ,RepAns_flag:=max(Rep_Ans_flag), by ="QueID"]

names(Alastdf)

# Removing unwanted columns
Alastdf$Repseqflag = NULL
Alastdf$Ansseqflag= NULL
Alastdf$Rep_Ans_flag = NULL

################# Question 5 - with max upvotes is accepted ##############

Aq5 = data.table(Alastdf)
Aq5[ ,Before_max:=max(Before_upvotes), by ="QueID"]

Aq5  = sqldf("select *,
            (case when Before_max = Before_upvotes then 1 else 0 end ) as Before_Max_Flag
            from Aq5 ")

Aq5 = data.table(Aq5)
Aq5[ ,Q5:=ifelse((Before_Max_Flag== 1 & IsAccepted==1),1 ,0), by ="QueID"]

Aq5final = sqldf("select distinct QueId,max(Q5) as que5 from Aq5  group by QueId ") # 67030 questions

# merging with previous file
Alastdf1 =  merge(x=Alastdf, y = Aq5final,by.x =c("QueID"),by.y = c("QueID"),all.x = T )

################################## Question 6 ##########################################
# spike in upvote
Afinal1 = sqldf("select QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
              QReputation,QueviewCount,AReputation,AnsReputation,QueTags,AfterAccepted ,
              AnsCreationdate,QueCreationDate,sum(AnsUpvotecount) as upvotes,sum(AnsDownvotecount) as downvotes from ADT 
              where VoteCreationDate is not 0  and Acceptdate is not 0 and votedays < 5
              group by QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
              AnsReputation,QueTags,AfterAccepted,QReputation,QueviewCount,AReputation,AnsCreationdate,QueCreationDate")


Afinalnew1 = sqldf("select QueID,AnsID,QueOwnerReputation,QReputation,QueviewCount,AReputation,
                 AnsReputation,QueTags,(case when QueacceptedanswerId=AnsID then 1 else 0 end) as IsAccepted,
                 AfterAccepted,upvotes,downvotes from Afinal1 ")

# separating before and upper votes
AAfterAcceptedVotes1  = Afinalnew1[Afinalnew1$AfterAccepted ==1,]

ABeforeAcceptedVotes1  = Afinalnew1[Afinalnew1$AfterAccepted ==0,]

Atest1 = merge(x=AAfterAcceptedVotes1 , y = ABeforeAcceptedVotes1, 
             by.x= c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation"),
             by.y =c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation"),
             all.x = T,all.y=T)

Atest1[is.na(Atest1)] <- 0

AAlldetails1 = sqldf("select QueID,AnsID,QueOwnerReputation,
                   AnsReputation,QueTags,IsAccepted  ,QReputation,QueviewCount,
                   [upvotes.x] As After_upvotes,[downvotes.x]  as After_downvotes,
                   [upvotes.y]  as Before_upvotes,[downvotes.y]  as Before_downvotes,
                   [upvotes.x] - [upvotes.y]  as upvote_diff,
                   [downvotes.x]-[downvotes.y]  as downvote_diff 
                   From  Atest1")

Alastdf2 = sqldf("select QueID,AnsID,IsAccepted,QueOwnerReputation,
               QReputation,AnsReputation,QueviewCount,
               After_upvotes,After_downvotes,Before_upvotes,(Before_upvotes+ After_upvotes) as totalupvotes,
               (After_downvotes+Before_downvotes) as totaldownvotes,
               Before_downvotes,upvote_diff,downvote_diff 
               from AAlldetails1 order by QueID ,AnsID")

Aq6 = sqldf("select *,
           (After_upvotes / totalupvotes) as ratio from Alastdf2 ")

Aq6[is.na(Aq6)] <- 0

Aq6 = data.table(Aq6)

Aq6[ ,spike_max:=max(ratio), by ="QueID"]

Aq6 = sqldf("select *,
           (case when spike_max = ratio then 1 else 0 end ) as spike_Max_Flag
           from Aq6 ")

Aq6 = data.table(Aq6)

Aq6[ ,Q6:=ifelse((spike_Max_Flag== 1 & IsAccepted==1),1 ,0), by ="QueID"]

Aq6final = sqldf("select distinct QueId,max(Q6) as que6 from Aq6  group by QueId ") # 67030 questions

Alastdf3 =  merge(x=Alastdf1, y = Aq6final,by.x =c("QueID"),by.y = c("QueID"),all.x = T )

names(Alastdf3)

#################################################################################
############################# csharp tags #######################################
#################################################################################
csharp = data[QueTags %like% "c#"]

# seleccting required data required
Ctotal = sqldf("select QueID,AnsID,QueAcceptedAnswerId,QueOwnerReputation,
               QReputation,QueviewCount,AnsCreationdate,QueCreationDate,AnsCommnetCount,
length(AnsBody) - length(replace(AnsBody, ' ', '')) + 1 ANswordcount,QueOwnerUserID,AnsUserID,
               AReputation,AnsReputation,QueTags,AnsAccepted,Rep_csharp as TagRep,csharp_silver as silver,csharp_gold as gold,
               csharp_bronze as bronze,AnsUpvotecount,AnsDownvotecount,AnsBounty,VoteCreationDate,
               (case when AnsAccepted = 1 then VoteCreationDate end) as Accepteddate,length(AnsBody) as LengthofBody,
               (case when AnsBody like '%<code>%' or AnsBody like '%<a href=%'  then 1 else 0 end) as flag
               from csharp")
Ctotal[is.na(Ctotal)] <- 0

# converting to datatable 
CDT = data.table(Ctotal)
CDT[ ,AfterAccepted:=cumsum(AnsAccepted), by ="QueID"]
CDT[ ,Acceptdate:=max(Accepteddate), by ="QueID"]

# creating vote days as diff b/w vote creation and acceptdate
CDT = sqldf("select *,(julianday(VoteCreationDate) - julianday(Acceptdate ))as votedays  from CDT ")

# filtering vote days
Cfinal = sqldf("select QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
               QReputation,QueviewCount,AReputation,AnsReputation,QueTags,AfterAccepted ,
               TagRep,silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID,
               AnsCreationdate,QueCreationDate,sum(AnsUpvotecount) as upvotes,sum(AnsDownvotecount) as downvotes,LengthofBody,flag from CDT 
               group by QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
               AnsReputation,QueTags,AfterAccepted,QReputation,QueviewCount,AReputation,AnsCreationdate,QueCreationDate,
               TagRep,silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID")

Cfinalnew = sqldf("select QueID,AnsID,QueOwnerReputation,QReputation,QueviewCount,AReputation,
                  TagRep,silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID,
                  AnsReputation,QueTags,(case when QueacceptedanswerId=AnsID then 1 else 0 end) as IsAccepted,
                  AfterAccepted,upvotes,downvotes,LengthofBody,flag from Cfinal ")

# separating before and upper votes
CAfterAcceptedVotes  = Cfinalnew[Afinalnew$AfterAccepted ==1,]

CBeforeAcceptedVotes  = Cfinalnew[Afinalnew$AfterAccepted ==0,]

Ctest = merge(x=CAfterAcceptedVotes , y = CBeforeAcceptedVotes, 
              by.x= c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation",
                      "TagRep","silver","gold","bronze","AnsCommnetCount","ANswordcount","QueOwnerUserID","AnsUserID","LengthofBody","flag"),
              by.y =c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation",
                      "TagRep","silver","gold","bronze","AnsCommnetCount","ANswordcount","QueOwnerUserID","AnsUserID","LengthofBody","flag"),
              all.x = T,all.y=T)

Ctest[is.na(Ctest)] <- 0

CAlldetails = sqldf("select QueID,AnsID,QueOwnerReputation,
                    AnsReputation,QueTags,IsAccepted  ,QReputation,QueviewCount,
                    silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID,
                    [upvotes.x] As After_upvotes,[downvotes.x]  as After_downvotes,
                    [upvotes.y]  as Before_upvotes,[downvotes.y]  as Before_downvotes,
                    [upvotes.x] - [upvotes.y]  as upvote_diff,
                    [downvotes.x]-[downvotes.y]  as downvote_diff,LengthofBody ,flag
                    From  Ctest")
############################### ANs sequence ###############

Cseq = sqldf("select   distinct AnsID,QueID,AReputation,TagRep,
             AnsCreationdate,QueCreationDate, (julianday(AnsCreationdate) - julianday(QueCreationDate)) as timediff from Cfinal")

Cseq = data.table(Cseq)

# new columns for ans sequence and rep sequence using rank function

Cseq[,Ansseq:=rank(AnsCreationdate,ties.method="first"),by=QueID]
Cseq[,Repseq:=rank(-TagRep,ties.method="first"),by=QueID]

# merging with all details 
Calltotal =  merge(x=CAlldetails, y = Cseq,by.x =c("AnsID","QueID"),by.y = c("AnsID","QueID"),all.x = T )

Clastdf = sqldf("select QueID,AnsID,IsAccepted,QueOwnerReputation,
                QReputation,AnsReputation, AReputation,QueviewCount,
                TagRep,silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID,
                After_upvotes,After_downvotes,Before_upvotes,(Before_upvotes+ After_upvotes) as totalupvotes,
                (After_downvotes+Before_downvotes) as totaldownvotes,
                Before_downvotes,upvote_diff,downvote_diff,timediff,Ansseq,Repseq,LengthofBody ,flag
                from Calltotal  order by QueID ,AnsID")

Clastdf = data.table(Clastdf)
Clastdf[ ,Has_Accepted_Answer:=max(IsAccepted), by ="QueID"]
Clastdf[ ,Answer_count :=length(unique(AnsID)), by ="QueID"]
Clastdf[ ,Tag_name := "Csharp" ]

Clastdf = sqldf("select*, (case when IsAccepted=1 and Repseq=1 then 1 else 0 end) as Repseqflag,
                (case when IsAccepted=1 and Ansseq=1 then 1 else 0 end) as Ansseqflag ,
                (case when IsAccepted=1 and Repseq=1 and Ansseq=1 then 1 else 0 end) as Rep_Ans_flag from Clastdf ")

Clastdf = data.table(Clastdf)
Clastdf[ ,Repseq_flag:=max(Repseqflag), by ="QueID"]
Clastdf[ ,Ansseq_flag:=max(Ansseqflag), by ="QueID"]
Clastdf[ ,RepAns_flag:=max(Rep_Ans_flag), by ="QueID"]

# Removing unwanted columns
Clastdf$Repseqflag = NULL
Clastdf$Ansseqflag= NULL
Clastdf$Rep_Ans_flag = NULL

################# Question 5 - with max upvotes is accepted ##############

Cq5 = data.table(Clastdf)
Cq5[ ,Before_max:=max(Before_upvotes), by ="QueID"]

Cq5  = sqldf("select *,
             (case when Before_max = Before_upvotes then 1 else 0 end ) as Before_Max_Flag
             from Cq5 ")

Cq5 = data.table(Cq5)

Cq5[ ,Q5:=ifelse((Before_Max_Flag== 1 & IsAccepted==1),1 ,0), by ="QueID"]

Cq5final = sqldf("select distinct QueId,max(Q5) as que5 from Cq5  group by QueId ") 

# merging with previous file
Clastdf1 =  merge(x=Clastdf, y = Cq5final,by.x =c("QueID"),by.y = c("QueID"),all.x = T )

################################## Question 6 ##########################################
# spike in upvote
Cfinal1 = sqldf("select QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
                QReputation,QueviewCount,AReputation,AnsReputation,QueTags,AfterAccepted ,
                AnsCreationdate,QueCreationDate,sum(AnsUpvotecount) as upvotes,sum(AnsDownvotecount) as downvotes from CDT 
                where VoteCreationDate is not 0  and Acceptdate is not 0 and votedays < 5
                group by QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
                AnsReputation,QueTags,AfterAccepted,QReputation,QueviewCount,AReputation,AnsCreationdate,QueCreationDate")

Cfinalnew1 = sqldf("select QueID,AnsID,QueOwnerReputation,QReputation,QueviewCount,AReputation,
                   AnsReputation,QueTags,(case when QueacceptedanswerId=AnsID then 1 else 0 end) as IsAccepted,
                   AfterAccepted,upvotes,downvotes from Cfinal1 ")

# separating before and upper votes
CAfterAcceptedVotes1  = Cfinalnew1[Cfinalnew1$AfterAccepted ==1,]

CBeforeAcceptedVotes1  = Cfinalnew1[Cfinalnew1$AfterAccepted ==0,]

Ctest1 = merge(x=CAfterAcceptedVotes1 , y = CBeforeAcceptedVotes1, 
               by.x= c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation"),
               by.y =c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation"),
               all.x = T,all.y=T)

Ctest1[is.na(Ctest1)] <- 0

CAlldetails1 = sqldf("select QueID,AnsID,QueOwnerReputation,
                     AnsReputation,QueTags,IsAccepted  ,QReputation,QueviewCount,
                     [upvotes.x] As After_upvotes,[downvotes.x]  as After_downvotes,
                     [upvotes.y]  as Before_upvotes,[downvotes.y]  as Before_downvotes,
                     [upvotes.x] - [upvotes.y]  as upvote_diff,
                     [downvotes.x]-[downvotes.y]  as downvote_diff 
                     From  Ctest1")

Clastdf2 = sqldf("select QueID,AnsID,IsAccepted,QueOwnerReputation,
                 QReputation,AnsReputation,QueviewCount,
                 After_upvotes,After_downvotes,Before_upvotes,(Before_upvotes+ After_upvotes) as totalupvotes,
                 (After_downvotes+Before_downvotes) as totaldownvotes,
                 Before_downvotes,upvote_diff,downvote_diff 
                 from CAlldetails1 order by QueID ,AnsID")

Cq6 = sqldf("select *,
            (After_upvotes / totalupvotes) as ratio from Clastdf2 ")

Cq6[is.na(Cq6)] <- 0

Cq6 = data.table(Cq6)

Cq6[ ,spike_max:=max(ratio), by ="QueID"]

Cq6 = sqldf("select *,
            (case when spike_max = ratio then 1 else 0 end ) as spike_Max_Flag
            from Cq6 ")

Cq6 = data.table(Cq6)

Cq6[ ,Q6:=ifelse((spike_Max_Flag== 1 & IsAccepted==1),1 ,0), by ="QueID"]

Cq6final = sqldf("select distinct QueId,max(Q6) as que6 from Cq6  group by QueId ") # 67030 questions

Clastdf3 =  merge(x=Clastdf1, y = Cq6final,by.x =c("QueID"),by.y = c("QueID"),all.x = T )

#################################################################################
############################# java tags #########################################
#################################################################################
java = data[QueTags %like% "java" ]

java = java[!(QueTags %like% "javascript")]

# seleccting required data required
Jtotal = sqldf("select QueID,AnsID,QueAcceptedAnswerId,QueOwnerReputation,
               QReputation,QueviewCount,AnsCreationdate,QueCreationDate,AnsCommnetCount,
length(AnsBody) - length(replace(AnsBody, ' ', '')) + 1 ANswordcount,QueOwnerUserID,AnsUserID,
               AReputation,AnsReputation,QueTags,AnsAccepted,Rep_java as TagRep,java_silver as silver,java_gold as gold,
               java_bronze as bronze,AnsUpvotecount,AnsDownvotecount,AnsBounty,VoteCreationDate,
               (case when AnsAccepted = 1 then VoteCreationDate end) as Accepteddate,length(AnsBody) as LengthofBody,
               (case when AnsBody like '%<code>%' or AnsBody like '%<a href=%'  then 1 else 0 end) as flag
               from java")

Jtotal[is.na(Jtotal)] <- 0

# converting to datatable 
JDT = data.table(Jtotal)
JDT[ ,AfterAccepted:=cumsum(AnsAccepted), by ="QueID"]
JDT[ ,Acceptdate:=max(Accepteddate), by ="QueID"]

# creating vote days as diff b/w vote creation and acceptdate
JDT = sqldf("select *,(julianday(VoteCreationDate) - julianday(Acceptdate ))as votedays  from JDT ")

# filtering vote days
Jfinal = sqldf("select QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
               QReputation,QueviewCount,AReputation,AnsReputation,QueTags,AfterAccepted ,
               TagRep,silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID,
               AnsCreationdate,QueCreationDate,sum(AnsUpvotecount) as upvotes,sum(AnsDownvotecount) as downvotes,LengthofBody,flag from JDT 
               group by QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
               AnsReputation,QueTags,AfterAccepted,QReputation,QueviewCount,AReputation,AnsCreationdate,QueCreationDate,
               TagRep,silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID")

Jfinalnew = sqldf("select QueID,AnsID,QueOwnerReputation,QReputation,QueviewCount,AReputation,
                  TagRep,silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID,
                  AnsReputation,QueTags,(case when QueacceptedanswerId=AnsID then 1 else 0 end) as IsAccepted,
                  AfterAccepted,upvotes,downvotes,LengthofBody,flag from Jfinal ")

# separating before and upper votes
JAfterAcceptedVotes  = Jfinalnew[Afinalnew$AfterAccepted ==1,]

JBeforeAcceptedVotes  = Jfinalnew[Afinalnew$AfterAccepted ==0,]

Jtest = merge(x=JAfterAcceptedVotes , y = JBeforeAcceptedVotes, 
              by.x= c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation",
                      "TagRep","silver","gold","bronze","AnsCommnetCount","ANswordcount","QueOwnerUserID","AnsUserID","LengthofBody","flag"),
              by.y =c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation",
                      "TagRep","silver","gold","bronze","AnsCommnetCount","ANswordcount","QueOwnerUserID","AnsUserID","LengthofBody","flag"),
              all.x = T,all.y=T)

Jtest[is.na(Jtest)] <- 0

JAlldetails = sqldf("select QueID,AnsID,QueOwnerReputation,
                    AnsReputation,QueTags,IsAccepted  ,QReputation,QueviewCount,
                   silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID,
                    [upvotes.x] As After_upvotes,[downvotes.x]  as After_downvotes,
                    [upvotes.y]  as Before_upvotes,[downvotes.y]  as Before_downvotes,
                    [upvotes.x] - [upvotes.y]  as upvote_diff,
                    [downvotes.x]-[downvotes.y]  as downvote_diff,LengthofBody ,flag
                    From  Jtest")

############################### ANs sequence ###############

Jseq = sqldf("select   distinct AnsID,QueID,AReputation, TagRep,
             AnsCreationdate,QueCreationDate, (julianday(AnsCreationdate) - julianday(QueCreationDate)) as timediff from Jfinal")

Jseq = data.table(Jseq)

# new columns for ans sequence and rep sequence using rank function

Jseq[,Ansseq:=rank(AnsCreationdate,ties.method="first"),by=QueID]

Jseq[,Repseq:=rank(-TagRep,ties.method="first"),by=QueID]

# merging with all details 
Jalltotal =  merge(x=JAlldetails, y = Jseq,by.x =c("AnsID","QueID"),by.y = c("AnsID","QueID"),all.x = T )

Jlastdf = sqldf("select QueID,AnsID,IsAccepted,QueOwnerReputation,
                QReputation,AnsReputation, AReputation,QueviewCount,QueOwnerUserID,AnsUserID,
                TagRep,silver,gold, bronze,AnsCommnetCount,ANswordcount,
                After_upvotes,After_downvotes,Before_upvotes,(Before_upvotes+ After_upvotes) as totalupvotes,
                (After_downvotes+Before_downvotes) as totaldownvotes,
                Before_downvotes,upvote_diff,downvote_diff,timediff,Ansseq,Repseq,LengthofBody,flag
                from Jalltotal  order by QueID ,AnsID")

Jlastdf = data.table(Jlastdf)

Jlastdf[ ,Has_Accepted_Answer:=max(IsAccepted), by ="QueID"]

Jlastdf[ ,Answer_count :=length(unique(AnsID)), by ="QueID"]

Jlastdf[ ,Tag_name := "java" ]

Jlastdf = sqldf("select*, (case when IsAccepted=1 and Repseq=1 then 1 else 0 end) as Repseqflag,
                (case when IsAccepted=1 and Ansseq=1 then 1 else 0 end) as Ansseqflag ,
                (case when IsAccepted=1 and Repseq=1 and Ansseq=1 then 1 else 0 end) as Rep_Ans_flag from Jlastdf ")

Jlastdf = data.table(Jlastdf)
Jlastdf[ ,Repseq_flag:=max(Repseqflag), by ="QueID"]
Jlastdf[ ,Ansseq_flag:=max(Ansseqflag), by ="QueID"]
Jlastdf[ ,RepAns_flag:=max(Rep_Ans_flag), by ="QueID"]

# Removing unwanted columns
Jlastdf$Repseqflag = NULL
Jlastdf$Ansseqflag= NULL
Jlastdf$Rep_Ans_flag = NULL

################# Question 5 - with max upvotes is accepted ##############

Jq5 = data.table(Jlastdf)

Jq5[ ,Before_max:=max(Before_upvotes), by ="QueID"]

Jq5  = sqldf("select *,
             (case when Before_max = Before_upvotes then 1 else 0 end ) as Before_Max_Flag
             from Jq5 ")

Jq5 = data.table(Jq5)

Jq5[ ,Q5:=ifelse((Before_Max_Flag== 1 & IsAccepted==1),1 ,0), by ="QueID"]

Jq5final = sqldf("select distinct QueId,max(Q5) as que5 from Jq5  group by QueId ") 

# merging with previous file
Jlastdf1 =  merge(x=Jlastdf, y = Jq5final,by.x =c("QueID"),by.y = c("QueID"),all.x = T )

################################## Question 6 ##########################################
# spike in upvote
Jfinal1 = sqldf("select QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
                QReputation,QueviewCount,AReputation,AnsReputation,QueTags,AfterAccepted ,
                AnsCreationdate,QueCreationDate,sum(AnsUpvotecount) as upvotes,sum(AnsDownvotecount) as downvotes from JDT 
                where VoteCreationDate is not 0  and Acceptdate is not 0 and votedays < 5
                group by QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
                AnsReputation,QueTags,AfterAccepted,QReputation,QueviewCount,AReputation,AnsCreationdate,QueCreationDate")

Jfinalnew1 = sqldf("select QueID,AnsID,QueOwnerReputation,QReputation,QueviewCount,AReputation,
                   AnsReputation,QueTags,(case when QueacceptedanswerId=AnsID then 1 else 0 end) as IsAccepted,
                   AfterAccepted,upvotes,downvotes from Jfinal1 ")

# separating before and upper votes

JAfterAcceptedVotes1  = Jfinalnew1[Jfinalnew1$AfterAccepted ==1,]

JBeforeAcceptedVotes1  = Jfinalnew1[Jfinalnew1$AfterAccepted ==0,]

Jtest1 = merge(x=JAfterAcceptedVotes1 , y = JBeforeAcceptedVotes1, 
               by.x= c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation"),
               by.y =c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation"),
               all.x = T,all.y=T)

Jtest1[is.na(Jtest1)] <- 0

JAlldetails1 = sqldf("select QueID,AnsID,QueOwnerReputation,
                     AnsReputation,QueTags,IsAccepted  ,QReputation,QueviewCount,
                     [upvotes.x] As After_upvotes,[downvotes.x]  as After_downvotes,
                     [upvotes.y]  as Before_upvotes,[downvotes.y]  as Before_downvotes,
                     [upvotes.x] - [upvotes.y]  as upvote_diff,
                     [downvotes.x]-[downvotes.y]  as downvote_diff 
                     From  Jtest1")

Jlastdf2 = sqldf("select QueID,AnsID,IsAccepted,QueOwnerReputation,
                 QReputation,AnsReputation,QueviewCount,
                 After_upvotes,After_downvotes,Before_upvotes,(Before_upvotes+ After_upvotes) as totalupvotes,
                 (After_downvotes+Before_downvotes) as totaldownvotes,
                 Before_downvotes,upvote_diff,downvote_diff 
                 from JAlldetails1 order by QueID ,AnsID")

Jq6 = sqldf("select *,
            (After_upvotes / totalupvotes) as ratio from Jlastdf2 ")

Jq6[is.na(Jq6)] <- 0

Jq6 = data.table(Jq6)

Jq6[ ,spike_max:=max(ratio), by ="QueID"]

Jq6 = sqldf("select *,
            (case when spike_max = ratio then 1 else 0 end ) as spike_Max_Flag
            from Jq6 ")

Jq6 = data.table(Jq6)

Jq6[ ,Q6:=ifelse((spike_Max_Flag== 1 & IsAccepted==1),1 ,0), by ="QueID"]

Jq6final = sqldf("select distinct QueId,max(Q6) as que6 from Jq6  group by QueId ") # 67030 questions

Jlastdf3 =  merge(x=Jlastdf1, y = Jq6final,by.x =c("QueID"),by.y = c("QueID"),all.x = T )

################################################################################
############################# javascript tags ##################################
################################################################################

javascript = data[QueTags %like% "javascript"]

# seleccting required data required
JStotal = sqldf("select QueID,AnsID,QueAcceptedAnswerId,QueOwnerReputation,
               QReputation,QueviewCount,AnsCreationdate,QueCreationDate,AnsCommnetCount,
length(AnsBody) - length(replace(AnsBody, ' ', '')) + 1 ANswordcount,QueOwnerUserID,AnsUserID,
               AReputation,AnsReputation,QueTags,AnsAccepted,Rep_javascript as TagRep,javascript_silver as silver,javascript_gold as gold,
               javascript_bronze as bronze,AnsUpvotecount,AnsDownvotecount,AnsBounty,VoteCreationDate,
               (case when AnsAccepted = 1 then VoteCreationDate end) as Accepteddate,length(AnsBody) as LengthofBody,
               (case when AnsBody like '%<code>%' or AnsBody like '%<a href=%'  then 1 else 0 end) as flag
               from javascript")

JStotal[is.na(JStotal)] <- 0

# converting to datatable 
JSDT = data.table(JStotal)

JSDT[ ,AfterAccepted:=cumsum(AnsAccepted), by ="QueID"]

JSDT[ ,Acceptdate:=max(Accepteddate), by ="QueID"]

# creating vote days as diff b/w vote creation and acceptdate
JSDT = sqldf("select *,(julianday(VoteCreationDate) - julianday(Acceptdate ))as votedays  from JSDT ")

# filtering vote days
JSfinal = sqldf("select QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
               QReputation,QueviewCount,AReputation,AnsReputation,QueTags,AfterAccepted ,
               TagRep,silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID,
               AnsCreationdate,QueCreationDate,sum(AnsUpvotecount) as upvotes,sum(AnsDownvotecount) as downvotes , LengthofBody,flag from JSDT 
               group by QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
               AnsReputation,QueTags,AfterAccepted,QReputation,QueviewCount,AReputation,AnsCreationdate,QueCreationDate,
               TagRep,silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID")

JSfinalnew = sqldf("select QueID,AnsID,QueOwnerReputation,QReputation,QueviewCount,AReputation,
                  TagRep,silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID,
                  AnsReputation,QueTags,(case when QueacceptedanswerId=AnsID then 1 else 0 end) as IsAccepted,
                  AfterAccepted,upvotes,downvotes,LengthofBody,flag from JSfinal ")

# separating before and upper votes

JSAfterAcceptedVotes  = JSfinalnew[Afinalnew$AfterAccepted ==1,]

JSBeforeAcceptedVotes  = JSfinalnew[Afinalnew$AfterAccepted ==0,]

JStest = merge(x=JSAfterAcceptedVotes , y = JSBeforeAcceptedVotes, 
              by.x= c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation",
                      "TagRep","silver","gold","bronze","AnsCommnetCount","ANswordcount","QueOwnerUserID","AnsUserID","LengthofBody","flag"),
              by.y =c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation",
                      "TagRep","silver","gold","bronze","AnsCommnetCount","ANswordcount","QueOwnerUserID","AnsUserID","LengthofBody","flag"),
              all.x = T,all.y=T)

JStest[is.na(JStest)] <- 0

JSAlldetails = sqldf("select QueID,AnsID,QueOwnerReputation,
                    AnsReputation,QueTags,IsAccepted  ,QReputation,QueviewCount,
                    silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID,
                    [upvotes.x] As After_upvotes,[downvotes.x]  as After_downvotes,
                    [upvotes.y]  as Before_upvotes,[downvotes.y]  as Before_downvotes,
                    [upvotes.x] - [upvotes.y]  as upvote_diff,
                    [downvotes.x]-[downvotes.y]  as downvote_diff,LengthofBody,flag 
                    From  JStest")
############################### ANs sequence ###############

JSseq = sqldf("select   distinct AnsID,QueID,AReputation,TagRep,
             AnsCreationdate,QueCreationDate, (julianday(AnsCreationdate) - julianday(QueCreationDate)) as timediff from JSfinal")

JSseq = data.table(JSseq)

# new columns for ans sequence and rep sequence using rank function

JSseq[,Ansseq:=rank(AnsCreationdate,ties.method="first"),by=QueID]

JSseq[,Repseq:=rank(-TagRep,ties.method="first"),by=QueID]

# merging with all details 
JSalltotal =  merge(x=JSAlldetails, y = JSseq,by.x =c("AnsID","QueID"),by.y = c("AnsID","QueID"),all.x = T )

JSlastdf = sqldf("select QueID,AnsID,IsAccepted,QueOwnerReputation,
                QReputation,AnsReputation, AReputation,QueviewCount,QueOwnerUserID,AnsUserID,
                TagRep,silver,gold, bronze,AnsCommnetCount,ANswordcount,
                After_upvotes,After_downvotes,Before_upvotes,(Before_upvotes+ After_upvotes) as totalupvotes,
                (After_downvotes+Before_downvotes) as totaldownvotes,
                Before_downvotes,upvote_diff,downvote_diff,timediff,Ansseq,Repseq,LengthofBody,flag
                from JSalltotal  order by QueID ,AnsID")

JSlastdf = data.table(JSlastdf)

JSlastdf[ ,Has_Accepted_Answer:=max(IsAccepted), by ="QueID"]

JSlastdf[ ,Answer_count :=length(unique(AnsID)), by ="QueID"]

JSlastdf[ ,Tag_name := "javascript" ]

JSlastdf = sqldf("select*, (case when IsAccepted=1 and Repseq=1 then 1 else 0 end) as Repseqflag,
                (case when IsAccepted=1 and Ansseq=1 then 1 else 0 end) as Ansseqflag ,
                (case when IsAccepted=1 and Repseq=1 and Ansseq=1 then 1 else 0 end) as Rep_Ans_flag from JSlastdf ")

JSlastdf = data.table(JSlastdf)
JSlastdf[ ,Repseq_flag:=max(Repseqflag), by ="QueID"]
JSlastdf[ ,Ansseq_flag:=max(Ansseqflag), by ="QueID"]
JSlastdf[ ,RepAns_flag:=max(Rep_Ans_flag), by ="QueID"]

# Removing unwanted columns
JSlastdf$Repseqflag = NULL
JSlastdf$Ansseqflag= NULL
JSlastdf$Rep_Ans_flag = NULL

names(JSlastdf)

################# Question 5 - with max upvotes is accepted ##############

JSq5 = data.table(JSlastdf)

JSq5[ ,Before_max:=max(Before_upvotes), by ="QueID"]

JSq5  = sqldf("select *,
             (case when Before_max = Before_upvotes then 1 else 0 end ) as Before_Max_Flag
             from JSq5 ")

JSq5 = data.table(JSq5)

JSq5[ ,Q5:=ifelse((Before_Max_Flag== 1 & IsAccepted==1),1 ,0), by ="QueID"]

JSq5final = sqldf("select distinct QueId,max(Q5) as que5 from JSq5  group by QueId ") 

# merging with previous file
JSlastdf1 =  merge(x=JSlastdf, y = JSq5final,by.x =c("QueID"),by.y = c("QueID"),all.x = T )

################################## Question 6 ##########################################
# spike in upvote
JSfinal1 = sqldf("select QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
                QReputation,QueviewCount,AReputation,AnsReputation,QueTags,AfterAccepted ,
                AnsCreationdate,QueCreationDate,sum(AnsUpvotecount) as upvotes,sum(AnsDownvotecount) as downvotes from JSDT 
                where VoteCreationDate is not 0  and Acceptdate is not 0 and votedays < 5
                group by QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
                AnsReputation,QueTags,AfterAccepted,QReputation,QueviewCount,AReputation,AnsCreationdate,QueCreationDate")

JSfinalnew1 = sqldf("select QueID,AnsID,QueOwnerReputation,QReputation,QueviewCount,AReputation,
                   AnsReputation,QueTags,(case when QueacceptedanswerId=AnsID then 1 else 0 end) as IsAccepted,
                   AfterAccepted,upvotes,downvotes from JSfinal1 ")

# separating before and upper votes

JSAfterAcceptedVotes1  = JSfinalnew1[JSfinalnew1$AfterAccepted ==1,]

JSBeforeAcceptedVotes1  = JSfinalnew1[JSfinalnew1$AfterAccepted ==0,]

JStest1 = merge(x=JSAfterAcceptedVotes1 , y = JSBeforeAcceptedVotes1, 
               by.x= c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation"),
               by.y =c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation"),
               all.x = T,all.y=T)

JStest1[is.na(JStest1)] <- 0

JSAlldetails1 = sqldf("select QueID,AnsID,QueOwnerReputation,
                     AnsReputation,QueTags,IsAccepted  ,QReputation,QueviewCount,
                     [upvotes.x] As After_upvotes,[downvotes.x]  as After_downvotes,
                     [upvotes.y]  as Before_upvotes,[downvotes.y]  as Before_downvotes,
                     [upvotes.x] - [upvotes.y]  as upvote_diff,
                     [downvotes.x]-[downvotes.y]  as downvote_diff 
                     From  JStest1")

JSlastdf2 = sqldf("select QueID,AnsID,IsAccepted,QueOwnerReputation,
                 QReputation,AnsReputation,QueviewCount,
                 After_upvotes,After_downvotes,Before_upvotes,(Before_upvotes+ After_upvotes) as totalupvotes,
                 (After_downvotes+Before_downvotes) as totaldownvotes,
                 Before_downvotes,upvote_diff,downvote_diff 
                 from JSAlldetails1 order by QueID ,AnsID")

JSq6 = sqldf("select *,
            (After_upvotes / totalupvotes) as ratio from JSlastdf2 ")

JSq6[is.na(JSq6)] <- 0

JSq6 = data.table(JSq6)

JSq6[ ,spike_max:=max(ratio), by ="QueID"]

JSq6 = sqldf("select *,
            (case when spike_max = ratio then 1 else 0 end ) as spike_Max_Flag
            from JSq6 ")

JSq6 = data.table(JSq6)

JSq6[ ,Q6:=ifelse((spike_Max_Flag== 1 & IsAccepted==1),1 ,0), by ="QueID"]

JSq6final = sqldf("select distinct QueId,max(Q6) as que6 from JSq6  group by QueId ") # 67030 questions

JSlastdf3 =  merge(x=JSlastdf1, y = JSq6final,by.x =c("QueID"),by.y = c("QueID"),all.x = T )

names(JSlastdf3)

##############################################################################
############################# php tags #######################################
##############################################################################
php = data[QueTags %like% "php"]

# seleccting required data required
Ptotal = sqldf("select QueID,AnsID,QueAcceptedAnswerId,QueOwnerReputation,
               QReputation,QueviewCount,AnsCreationdate,QueCreationDate,AnsCommnetCount,
length(AnsBody) - length(replace(AnsBody, ' ', '')) + 1 ANswordcount,QueOwnerUserID,AnsUserID,
               AReputation,AnsReputation,QueTags,AnsAccepted,Rep_csharp as TagRep,csharp_silver as silver,csharp_gold as gold,
               csharp_bronze as bronze,AnsUpvotecount,AnsDownvotecount,AnsBounty,VoteCreationDate,
               (case when AnsAccepted = 1 then VoteCreationDate end) as Accepteddate,length(AnsBody) as LengthofBody,
               (case when AnsBody like '%<code>%' or AnsBody like '%<a href=%'  then 1 else 0 end) as flag
               from php")

Ptotal[is.na(Ptotal)] <- 0

# converting to datatable 
PDT = data.table(Ptotal)
PDT[ ,AfterAccepted:=cumsum(AnsAccepted), by ="QueID"]
PDT[ ,Acceptdate:=max(Accepteddate), by ="QueID"]

# creating vote days as diff b/w vote creation and acceptdate
PDT = sqldf("select *,(julianday(VoteCreationDate) - julianday(Acceptdate ))as votedays  from PDT ")


# filtering vote days
Pfinal = sqldf("select QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
               QReputation,QueviewCount,AReputation,AnsReputation,QueTags,AfterAccepted ,
               TagRep,silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID,
               AnsCreationdate,QueCreationDate,sum(AnsUpvotecount) as upvotes,sum(AnsDownvotecount) as downvotes,LengthofBody,flag from PDT 
               group by QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
               AnsReputation,QueTags,AfterAccepted,QReputation,QueviewCount,AReputation,AnsCreationdate,QueCreationDate,
               TagRep,silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID")

Pfinalnew = sqldf("select QueID,AnsID,QueOwnerReputation,QReputation,QueviewCount,AReputation,
                  TagRep,silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID,
                  AnsReputation,QueTags,(case when QueacceptedanswerId=AnsID then 1 else 0 end) as IsAccepted,
                  AfterAccepted,upvotes,downvotes,LengthofBody,flag from Pfinal ")

# separating before and upper votes

PAfterAcceptedVotes  = Pfinalnew[Afinalnew$AfterAccepted ==1,]

PBeforeAcceptedVotes  = Pfinalnew[Afinalnew$AfterAccepted ==0,]

Ptest = merge(x=PAfterAcceptedVotes , y = PBeforeAcceptedVotes, 
               by.x= c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation",
                       "TagRep","silver","gold","bronze","AnsCommnetCount","ANswordcount","QueOwnerUserID","AnsUserID","LengthofBody","flag"),
               by.y =c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation",
                       "TagRep","silver","gold","bronze","AnsCommnetCount","ANswordcount","QueOwnerUserID","AnsUserID","LengthofBody","flag"),
               all.x = T,all.y=T)

Ptest[is.na(Ptest)] <- 0

PAlldetails = sqldf("select QueID,AnsID,QueOwnerReputation,
                    AnsReputation,QueTags,IsAccepted  ,QReputation,QueviewCount,
                    silver,gold, bronze,AnsCommnetCount,ANswordcount,QueOwnerUserID,AnsUserID,
                    [upvotes.x] As After_upvotes,[downvotes.x]  as After_downvotes,
                    [upvotes.y]  as Before_upvotes,[downvotes.y]  as Before_downvotes,
                    [upvotes.x] - [upvotes.y]  as upvote_diff,
                    [downvotes.x]-[downvotes.y]  as downvote_diff,LengthofBody,flag 
                    From  Ptest")

############################### ANs sequence ###############

Pseq = sqldf("select   distinct AnsID,QueID,AReputation,TagRep,
             AnsCreationdate,QueCreationDate, (julianday(AnsCreationdate) - julianday(QueCreationDate)) as timediff from Pfinal")

Pseq = data.table(Pseq)

# new columns for ans sequence and rep sequence using rank function
Pseq[,Ansseq:=rank(AnsCreationdate,ties.method="first"),by=QueID]

Pseq[,Repseq:=rank(-TagRep,ties.method="first"),by=QueID]

# merging with all details 
Palltotal =  merge(x=PAlldetails, y = Pseq,by.x =c("AnsID","QueID"),by.y = c("AnsID","QueID"),all.x = T )

Plastdf = sqldf("select QueID,AnsID,IsAccepted,QueOwnerReputation,
                QReputation,AnsReputation, AReputation,QueviewCount,QueOwnerUserID,AnsUserID,
                TagRep,silver,gold, bronze,AnsCommnetCount,ANswordcount,
                After_upvotes,After_downvotes,Before_upvotes,(Before_upvotes+ After_upvotes) as totalupvotes,
                (After_downvotes+Before_downvotes) as totaldownvotes,
                Before_downvotes,upvote_diff,downvote_diff,timediff,Ansseq,Repseq,LengthofBody ,flag
                from Palltotal  order by QueID ,AnsID")

Plastdf = data.table(Plastdf)
Plastdf[ ,Has_Accepted_Answer:=max(IsAccepted), by ="QueID"]
Plastdf[ ,Answer_count :=length(unique(AnsID)), by ="QueID"]
Plastdf[ ,Tag_name := "php" ]

Plastdf = sqldf("select*, (case when IsAccepted=1 and Repseq=1 then 1 else 0 end) as Repseqflag,
                (case when IsAccepted=1 and Ansseq=1 then 1 else 0 end) as Ansseqflag ,
                (case when IsAccepted=1 and Repseq=1 and Ansseq=1 then 1 else 0 end) as Rep_Ans_flag from Plastdf ")

Plastdf = data.table(Plastdf)
Plastdf[ ,Repseq_flag:=max(Repseqflag), by ="QueID"]
Plastdf[ ,Ansseq_flag:=max(Ansseqflag), by ="QueID"]
Plastdf[ ,RepAns_flag:=max(Rep_Ans_flag), by ="QueID"]

# Removing unwanted columns
Plastdf$Repseqflag = NULL
Plastdf$Ansseqflag= NULL
Plastdf$Rep_Ans_flag = NULL

################# Question 5 - with max upvotes is accepted ##############

Pq5 = data.table(Plastdf)

Pq5[ ,Before_max:=max(Before_upvotes), by ="QueID"]

Pq5  = sqldf("select *,
             (case when Before_max = Before_upvotes then 1 else 0 end ) as Before_Max_Flag
             from Pq5 ")

Pq5 = data.table(Pq5)

Pq5[ ,Q5:=ifelse((Before_Max_Flag== 1 & IsAccepted==1),1 ,0), by ="QueID"]

Pq5final = sqldf("select distinct QueId,max(Q5) as que5 from Pq5  group by QueId ") 

# merging with previous file
Plastdf1 =  merge(x=Plastdf, y = Pq5final,by.x =c("QueID"),by.y = c("QueID"),all.x = T )

################################## Question 6 ##########################################
# spike in upvote
Pfinal1 = sqldf("select QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
                QReputation,QueviewCount,AReputation,AnsReputation,QueTags,AfterAccepted ,
                AnsCreationdate,QueCreationDate,sum(AnsUpvotecount) as upvotes,sum(AnsDownvotecount) as downvotes from PDT 
                where VoteCreationDate is not 0  and Acceptdate is not 0 and votedays < 5
                group by QueID,AnsID,QueacceptedanswerId,QueOwnerReputation,
                AnsReputation,QueTags,AfterAccepted,QReputation,QueviewCount,AReputation,AnsCreationdate,QueCreationDate")

Pfinalnew1 = sqldf("select QueID,AnsID,QueOwnerReputation,QReputation,QueviewCount,AReputation,
                   AnsReputation,QueTags,(case when QueacceptedanswerId=AnsID then 1 else 0 end) as IsAccepted,
                   AfterAccepted,upvotes,downvotes from Pfinal1 ")

# separating before and upper votes

PAfterAcceptedVotes1  = Pfinalnew1[Pfinalnew1$AfterAccepted ==1,]

PBeforeAcceptedVotes1  = Pfinalnew1[Pfinalnew1$AfterAccepted ==0,]

Ptest1 = merge(x=PAfterAcceptedVotes1 , y = PBeforeAcceptedVotes1, 
               by.x= c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation"),
               by.y =c("QueID","AnsID","QueOwnerReputation","AnsReputation","QueTags","IsAccepted","QReputation","QueviewCount","AReputation"),
               all.x = T,all.y=T)

Ptest1[is.na(Ptest1)] <- 0

PAlldetails1 = sqldf("select QueID,AnsID,QueOwnerReputation,
                     AnsReputation,QueTags,IsAccepted  ,QReputation,QueviewCount,
                     [upvotes.x] As After_upvotes,[downvotes.x]  as After_downvotes,
                     [upvotes.y]  as Before_upvotes,[downvotes.y]  as Before_downvotes,
                     [upvotes.x] - [upvotes.y]  as upvote_diff,
                     [downvotes.x]-[downvotes.y]  as downvote_diff 
                     From  Ptest1")

Plastdf2 = sqldf("select QueID,AnsID,IsAccepted,QueOwnerReputation,
                 QReputation,AnsReputation,QueviewCount,
                 After_upvotes,After_downvotes,Before_upvotes,(Before_upvotes+ After_upvotes) as totalupvotes,
                 (After_downvotes+Before_downvotes) as totaldownvotes,
                 Before_downvotes,upvote_diff,downvote_diff 
                 from PAlldetails1 order by QueID ,AnsID")

Pq6 = sqldf("select *,
            (After_upvotes / totalupvotes) as ratio from Plastdf2 ")

Pq6[is.na(Pq6)] <- 0

Pq6 = data.table(Pq6)

Pq6[ ,spike_max:=max(ratio), by ="QueID"]

Pq6 = sqldf("select *,
            (case when spike_max = ratio then 1 else 0 end ) as spike_Max_Flag
            from Pq6 ")

Pq6 = data.table(Pq6)

Pq6[ ,Q6:=ifelse((spike_Max_Flag== 1 & IsAccepted==1),1 ,0), by ="QueID"]

Pq6final = sqldf("select distinct QueId,max(Q6) as que6 from Pq6  group by QueId ") # 67030 questions

Plastdf3 =  merge(x=Plastdf1, y = Pq6final,by.x =c("QueID"),by.y = c("QueID"),all.x = T )


#################################################################################

# combing all tag wise analysis
tagwisedata = rbind(Alastdf3,Clastdf3,Jlastdf3,JSlastdf3,Plastdf3)

# writing the final file 
write.csv(tagwisedata,"D:\\Python\\Project\\NIlesh\\Final\\finaldataframe.csv",row.names = F)

