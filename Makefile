CLASS=WordCount
IN=/in.txt
OUT=/out

all:
	javac *.java -cp $(shell hadoop classpath)
	jar cf $(CLASS).jar *.class

run:
	hadoop fs -rm -r -f $(OUT)
	yarn jar $(CLASS).jar $(CLASS) $(IN) $(OUT)

out:
	hadoop fs -cat $(OUT)/part-r-00000 | less

clean:
	rm *.class *.jar
