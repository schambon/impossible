library("ggplot2")
library("dplyr")
library("magrittr")


microsharded1 <- read.table("../microsharded_1_full.txt", header=TRUE, sep="\t", dec=".", stringsAsFactors = F)
microsharded2 <- read.table("../microsharded_2_full.txt", header=TRUE, sep="\t", dec=".", stringsAsFactors = F)
single2 <- read.table("../single_2_full.txt", header=T, sep="\t", dec=".", stringsAsFactors = F)
single1 <- read.table("../single_1_full.txt", header=T, sep="\t", dec=".", stringsAsFactors = F)


microsharded1$Environment = "Microsharded"
microsharded1$Index = "Compound"
microsharded2$Environment = "Microsharded"
microsharded2$Index = "Single"
single2$Environment = "Single"
single2$Index = "Single"
single1$Environment = "Single"
single1$Index = "Compound"

# data <- do.call(rbind, list(microsharded1 %>% select(Case, Test, mean, Environment, Index), microsharded2 %>% select(Case, Test, mean, Environment, Index), single2 %>% select(Case, Test, mean, Environment, Index), single1 %>% select(Case, Test, mean, Environment, Index)))
data <- do.call(rbind, list(microsharded1 %>% select(Case,Test,mean,countMean, Environment,Index), microsharded2 %>% select(Case,Test,mean,countMean, Environment,Index),single1 %>% select(Case,Test,mean,countMean,Environment,Index), single2 %>% select(Case,Test,mean,countMean,Environment,Index))) %>% mutate(page = mean - countMean)

plot <- ggplot(data, aes(Test, mean)) + geom_bar(aes(fill=Case), alpha=0.5, stat="identity", position="dodge") + geom_bar(aes(y=page, fill=Case), stat="identity", position="dodge") + facet_grid(Index ~ Environment) + theme(axis.text.x = element_text(angle = 90, hjust = 1))
print(plot)

# plot2 <- plot + scale_y_continuous(trans="log10")
# print(plot2)

without.slow <- data %>% filter(Case != "SortFirst" & Case != "CollScanAgg")
plot3 <- ggplot(without.slow, aes(Test, mean)) + geom_bar(aes(fill=Case), alpha=0.5, stat="identity", position="dodge") + geom_bar(aes(Test,page, fill=Case), stat="identity", position="dodge") + facet_grid(Index ~ Environment) + scale_y_continuous(trans="log10")+ geom_hline(yintercept=500) + theme(axis.text.x = element_text(angle = 90, hjust = 1))
print(plot3)

ixscan <- data %>% filter(Case == "IxScanAgg") %>% mutate(Situation = paste(Environment, Index, sep="-"))
plot4 <- ggplot(ixscan, aes(Test, mean)) + geom_bar(aes(fill=Situation), alpha=0.5, stat="identity", position="dodge") + geom_bar(aes(Test,page,fill=Situation), stat="identity", position="dodge") + scale_y_continuous(trans="log10") + geom_hline(yintercept=500) + theme(axis.text.x = element_text(angle = 90, hjust = 1))
print(plot4)

ggsave(filename = "../general.png", plot=plot, units="cm", width=30, height=15)
ggsave(filename = "../faster.png", plot=plot3, units="cm", width=30, height=15)
ggsave(filename = "../ixscan.png", plot=plot4, units="cm", width=30, height=15)
