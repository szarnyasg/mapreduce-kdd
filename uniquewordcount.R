# please set the working directory to the script's directory

# http://stackoverflow.com/questions/2885394/whats-the-best-way-to-count-unique-visitors-with-hadoop

# You could do it as a 2-stage operation:
# First step, emit (username => siteID), and have the reducer just collapse multiple occurrences of siteID using a set - since you'd typically have far less sites than users, this should be fine.
# Then in the second step, you can emit (siteID => username) and do a simple count, since the duplicates have been removed.

# rmr2: https://github.com/RevolutionAnalytics/RHadoop/wiki/user-rmr-Writing-composable-mapreduce-jobs

library(rmr2)
rmr.options(backend="local")

uniquewordcount = 
	function(
		input, 
		output = NULL,
		pattern = " "){
		
		# phase1
		phase1.map = 
			function(., lines) {
				keyval(
					unlist(
						strsplit(
							x = lines,
							split = pattern)),
					1)
			}
		
		phase1.reduce =
			function(.k, .vv) {
				keyval(0, 1)
			}
		
		# phase2
		phase2.map =
			function(., v) {
				keyval(0, length(v))
			}
		
		phase2.reduce =
			function(., vv) {
				keyval(0, sum(vv))
			}
		
		mapreduce(
			input =
				mapreduce(		
					input = input,
					output = output,
					input.format = "text",
					map = phase1.map,
					reduce = phase1.reduce,
					combine = TRUE
				),
			output = output,
			map = phase2.map,
 			reduce = phase2.reduce,
			combine = TRUE
		)}

a <- uniquewordcount("text.txt")
b <- from.dfs(a)
print(b$val)
