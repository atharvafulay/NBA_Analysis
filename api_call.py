import xml.etree.ElementTree as ET

# from https://stackoverflow.com/a/25920989
# instead of ET.fromstring(xml)
it = ET.iterparse("/Users/Atharva/Documents/Github/NBA_Analysis/nba-team-season-stats-sample.xml")
for _, el in it:
    if '}' in el.tag:
        el.tag = el.tag.split('}', 1)[1]  # strip all namespaces
root = it.root

for team in root.iter('team'):
    print(team.attrib.keys())

    for total in team.iter('total'):
        print(total.attrib.keys())

    for average in team.iter('average'):
        print(average.attrib.keys())



for player in root.iter('player'):
    print(player.attrib)

    for total in player.iter('total'):
        print(total.attrib)

    for average in player.iter('average'):
        print(average.attrib)
    break

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
