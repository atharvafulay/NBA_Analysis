import xml.etree.ElementTree as ET

# from https://stackoverflow.com/a/25920989
# instead of ET.fromstring(xml)
it = ET.iterparse("C:/Users/Atharva/Downloads/nba-team-season-stats-sample.xml")
for _, el in it:
    if '}' in el.tag:
        el.tag = el.tag.split('}', 1)[1]  # strip all namespaces
root = it.root

print(root.tag)

for player in root.iter('player'):
    print('something')
    print(player.attrib)



# https://docs.python.org/3/library/xml.etree.elementtree.html
