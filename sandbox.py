# obj would be "team_records" or "player_records", this will limit it to the correct node children
for obj in root.iter(feeds[0]):

    for item in obj.iter('player'):
        print(item.attrib['id'])

        player_id = item.attrib['id']

        # Within the _records nodes, we pull the total and average for each entity.
        # this logic is the same as the loop above, just that it is using feeds as the filter in iter()
        for total in obj.iter(feeds[1]):
            if len(attrib_total_keys) == 0:
                attrib_total_keys = (list(total.attrib.keys()))
                attrib_total_keys.append('player_id')

            total_list = list(total.attrib.values()) + list()
            total_list.append(player_id)

            print(total_list)
            attrib_total_values.append(total_list)
            # print(total.tag)

        for average in obj.iter(feeds[2]):
            if len(attrib_avg_keys) == 0:
                attrib_avg_keys = (list(average.attrib.keys()))
                attrib_avg_keys.append('player_id')

            avg_list = list(average.attrib.values())
            avg_list.append(player_id)
            attrib_total_values.append(avg_list)

# print(attrib_total_values)

# convert the list of lists of values, and keys into a DF, feed this to the insert functions
entity_df = pd.DataFrame(attrib_entity_values, columns=attrib_entity_keys)
total_df = pd.DataFrame(attrib_total_values, columns=attrib_total_keys)
average_df = pd.DataFrame(attrib_avg_values, columns=attrib_avg_keys)