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

df <- read.table(file = smalldata, header = TRUE, sep = "\t")

masterdata<- "algebra_2005_2006/algebra_2005_2006_master.txt"
masterdata_nohdr <- "algebra_2005_2006/algebra_2005_2006_master_no_header.txt"

df <- read.table(file = masterdata, header = TRUE, sep = "\t")
View(df)

###
### MapReduce job for enumerating the students
###
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

###
### 2-phase MapReduce job to count the number of students
###
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

###
### MapReduce job to determine how much time each student spent with the problems [seconds]
###
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


###
### MapReduce job for calculating the average success rate for each student
###
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

score <- studentscorrectfun(masterdata_nohdr)

###
### MapReduce job to determine the time each student spent with the problems [seconds]
###
studentstime = 
	function(input, output = NULL, pattern = "\n") {
		
		st.map = 
			function(., lines) {
				df <- read.table(text = lines, sep = "\t", header = FALSE)
				names(df) <- geomcols
				keyval(df["Anon.Student.Id"], df["Step.Duration..sec."])
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

time <- studentstimefun(masterdata_nohdr)

ihist(score$val, main="x: average score, y: number of students")
ihist(time$val, main="x: total time spent [s], y: number of students")

###
### MapReduce job for tracking a student's performance
###
studentsperf = 
	function(input, output = NULL, pattern = "\n") {
		
		st.map = 
			function(., lines) {
				df <- read.table(text = lines, sep = "\t", header = FALSE)
				names(df) <- geomcols
				keyval(df$Anon.Student.Id, df$Correct.First.Attempt)
			}
		
		st.reduce =
			function(k, vv) {
				part1 <- vv[1:(length(vv)/2)]
				part2 <- vv[(length(vv)/2+1):length(vv)]

				m1 <- mean(part1, na.rm = T)
				m2 <- mean(part2, na.rm = T)
				
				keyval(k, c(m1, m2))
			}
		
		mapreduce(		
			input = input,
			output = output,
			input.format = "text",
			map = st.map,
			reduce = st.reduce,
			combine = T)
	}

studentsperffun = function(file) {
	a <- studentsperf(file)
	b <- from.dfs(a)
	b
}

p <- studentsperffun(masterdata_nohdr)
p


odd <- c(TRUE, FALSE)
even <- c(FALSE, TRUE)

r1 <- p$val[odd]
r2 <- p$val[even]

r1
r2

cor(r1, r2)

###
### MapReduce job for counting the mean score for each problem
###
problemmeanscore = 
	function(input, output = NULL, pattern = "\n") {
		
		st.map = 
			function(., lines) {
				df <- read.table(text = lines, sep = "\t", header = FALSE)
				names(df) <- geomcols
				k <- df[,c("Problem.Hierarchy", "Problem.Name")]
				v <- df$Correct.First.Attempt
				keyval(k, v)
			}
		
		st.reduce =
			function(k, vv) {
				m <- mean(vv, na.rm = T)
				keyval(k, m)
			}
		
		mapreduce(		
			input = input,
			output = output,
			input.format = "text",
			map = st.map,
			reduce = st.reduce,
			combine = T)
	}

problemmeanscorefun = function(file) {
	a <- problemmeanscore(file)
	b <- from.dfs(a)
	b
}

problemscore <- problemmeanscorefun(masterdata_nohdr)
score

library(ggplot2)
ggplot(NULL, aes(x=score$val)) + geom_histogram()
