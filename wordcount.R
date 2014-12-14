# please set the working directory to the script's directory

library(rmr2)
rmr.options(backend="local")

wordcount = 
	function(
		input, 
		output = NULL,
		pattern = " "){
		
		wc.map = 
			function(., lines) {
				keyval(
					unlist(
						strsplit(
							x = lines,
							split = pattern)),
					1)}
		
		wc.reduce =
			function(word, counts) {
				keyval(word, sum(counts))}
		
		mapreduce(
			input = input ,
			output = output,
			input.format = "text",
			map = wc.map,
			reduce = wc.reduce,
			combine = T)}

a <- wordcount("text.txt")
b <- from.dfs(a)
print(b)
