setwd("/home/szarnyasg/git/mapreduce-kdd");

# initialization
# MapReduce
library(rmr2)
rmr.options(backend="local")

# visualization
library(iplots)

# KDD data set
geomcols <- c('Row', 'Anon.Student.Id', 'Problem.Hierarchy', 'Problem.Name', 'Problem.View', 'Step.Name', 'Step.Start.Time', 'First.Transaction.Time', 'Correct.Transaction.Time', 'Step.End.Time', 'Step.Duration..sec.', 'Correct.Step.Duration..sec.', 'Error.Step.Duration..sec.', 'Correct.First.Attempt', 'Incorrects', 'Hints', 'Corrects', 'KC.Default.', 'Opportunity.Default.')

# files
smalldata <- "algebra_2005_2006/algebra_2005_2006_master_small.txt"
smalldata_nohdr <- "algebra_2005_2006/algebra_2005_2006_master_small_no_header.txt"

masterdata<- "algebra_2005_2006/algebra_2005_2006_master.txt"
masterdata_nohdr <- "algebra_2005_2006/algebra_2005_2006_master_no_header.txt"

### MapReduce job for enumerating the students
studentids = 
	function(input, output = NULL, pattern = "\n") {
		
		wc.map = 
			function(., lines) {
				df <- read.table(text = lines, sep = "\t", header = FALSE)
				names(df) <- geomcols
				students <- unique(df["Anon.Student.Id"])
				keyval(students, 0)
			}
		
		mapreduce(		
			input = input,
			output = output,
			input.format = "text",
			map = wc.map,
			combine = T)
}


studentidsfun = function(file) {
	a <- studentids(file)
	b <- from.dfs(a)
	b
}

a <- studentidsfun(masterdata_nohdr)
a
length(unlist(keys(a)))

#stlist <- studentlistfun(masterdata_nohdr)
#stlist
#k <- keys(stlist)
#k
#length(unlist(k))

### count the number of students with a 2-phase MapReduce job
studentcount = 
	function(input, output = NULL, pattern = "\n") {
		
		phase1.map = 
			function(., lines) {
				df <- read.table(text = lines, sep = "\t", header = FALSE)
				names(df) <- geomcols
				students <- unique(df["Anon.Student.Id"])
				keyval(students, 0)
			}

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
					combine = T
				),
			output = output,
			map = phase2.map,
			reduce = phase2.reduce,
			combine = T
		)
	}

studentcountfun = function(file) {
	a <- studentcount(file)
	b <- from.dfs(a)
	b
}

a <- studentcountfun(masterdata_nohdr)
a


### determine how much time each student spent with the problems [seconds]
studentstime = 
	function(input, output = NULL, pattern = "\n") {
		
		st.map = 
			function(., lines) {
				df <- read.table(text = lines, sep = "\t", header = FALSE)
				names(df) <- geomcols
				keyval(df$Anon.Student.Id, df$Step.Duration..sec.)
			}
		
		st.reduce =
			function(k, vv) {
				keyval(k, sum(vv))
			}
		
		mapreduce(		
			input = input,
			output = output,
			input.format = "text",
			map = st.map,
			reduce = st.reduce,
			combine = T)
	}

studentstimefun = function(file) {
	a <- studentstime(file)
	b <- from.dfs(a)
	b
}

a <- studentstimefun(masterdata_nohdr)
a



### MapReduce job for calculating the average "grade for each student"
studentscorrect = 
	function(input, output = NULL, pattern = "\n") {
		
		st.map = 
			function(., lines) {
				df <- read.table(text = lines, sep = "\t", header = FALSE)
				names(df) <- geomcols
				keyval(df$Anon.Student.Id, df$Correct.First.Attempt)
			}
		
		st.reduce =
			function(k, vv) {
				keyval(k, mean(vv, na.rm = TRUE))
			}
		
		mapreduce(		
			input = input,
			output = output,
			input.format = "text",
			map = st.map,
			reduce = st.reduce,
			combine = T)
	}

studentscorrectfun = function(file) {
	a <- studentscorrect(file)
	b <- from.dfs(a)
	b
}

c <- studentscorrectfun(masterdata_nohdr)
c

ihist(c$val)
ihist(a$val)



### determine how much time each student spent with the problems [seconds]
studentstime = 
	function(input, output = NULL, pattern = "\n") {
		
		st.map = 
			function(., lines) {
				df <- read.table(text = lines, sep = "\t", header = FALSE)
				names(df) <- geomcols
				keyval(df["Anon.Student.Id"], df$["Step.Duration..sec."])
			}
		
		st.reduce =
			function(k, vv) {
				keyval(k, sum(vv))
			}
		
		mapreduce(		
			input = input,
			output = output,
			input.format = "text",
			map = st.map,
			reduce = st.reduce,
			combine = T)
	}

studentstimefun = function(file) {
	a <- studentstime(file)
	b <- from.dfs(a)
	b
}

a <- studentstimefun(masterdata_nohdr)
a

df <- read.table(file = masterdata, header = TRUE, sep = "\t")
d <- df$Step.Duration..sec.
d <- df["Step.Duration..sec."]
mean(d, na.rm = TRUE)

