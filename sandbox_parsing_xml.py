import xml.etree.ElementTree as ET

# from https://stackoverflow.com/a/25920989
# instead of ET.fromstring(xml)
it = ET.iterparse("/Users/Atharva/Documents/Github/NBA_Analysis/nba-team-season-stats-sample.xml")
for _, el in it:
    if '}' in el.tag:
        el.tag = el.tag.split('}', 1)[1]  # strip all namespaces
root = it.root

print(root.findall('team'))
print(ET.dump(root))

for team in root.iter('team_records'):
    print(ET.dump(team))


    print(team.children())
        # print(total['total'])
        # print('TYPE ^', total.attrib.keys())

    for average in team.iter('average'):
        print(average.attrib.keys())



# for player in root.iter('player'):
#     print(player.attrib.values())
#
#     test = player.attrib.values
#     print(test)
#
#     for total in player.iter('total'):
#         print(total.attrib)
#
#     for average in player.iter('average'):
#         print(average.attrib)
#     break

for player in root.iter('team'):
    print(player.attrib.values())

    test = player.attrib.values
    print(test)

    attrib_entity = list()
    attrib_total = list()
    attrib_avg = list()

    attrib_entity.append(list(player.attrib.values()))

    for total in player.iter('total'):
        attrib_total.append(list(total.attrib.values()))

    for average in player.iter('average'):
        attrib_avg.append(list(average.attrib.values()))



    print(attrib_entity)
    print(attrib_total)
    print(attrib_avg)


    # for total in player.iter('total'):
    #     print(total.attrib)
    #
    # for average in player.iter('average'):
    #     print(average.attrib)






# tree = ET.parse("/Users/Atharva/Documents/Github/NBA_Analysis/nba-team-season-stats-sample.xml")
#
# elemList = []
# for elem in tree.iter():
#   elemList.append(elem.tag) # indent this by tab, not two spaces as I did here
#
# # now I remove duplicities - by convertion to set and back to list
# elemList = list(set(elemList))
#
# # Just printing out the result
# print(elemList)

# https://docs.python.org/3/library/xml.etree.elementtree.html
