#player_id,game_id,date_created,review_id,voted_up
#output -> date_created,game_id,nbr_review_positive,nbr_review_negative
import numpy as np

def drop_first_line(index, itr):
    if index == 0:
        return iter(list(itr)[1:])
    else:
        return itr
      
input = sc.textFile('/FileStore/tables/game_review.csv', minPartitions=2).mapPartitionsWithIndex(drop_first_line)

clean_data = input.map(lambda x: x.split(',')).filter(lambda x: x[4].lower() in ['true','false','0','1'])

result = clean_data.map(lambda x: ((x[1],x[2]),x[4].lower().replace('true', '1').replace('false', '0')))
.map(lambda (a,b): (a, (1,0) if b=='1' else (0,1))).reduceByKey(lambda a,b: [(x + y) for x, y in zip(a, b)])

result.collect()
