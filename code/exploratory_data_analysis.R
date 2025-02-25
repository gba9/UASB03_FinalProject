### Load libraries
library(dplyr)
library(tidyr)
library(janitor)
library(lubridate)
library(ggplot2)
library(SmartEDA)
library(ggstatsplot)
library(RColorBrewer)
library(GGally)
library(viridis)
library(car)
library(stringr)
library(vcd)
library(paletteer)
library(patchwork) 
library("FactoMineR")
library("factoextra")

# INITIALIZATION ===

### Load data

wd <- getwd()
name<-'crispr_collection'
data <- read.csv(paste(wd, "/data_export/data_", name, ".csv", sep = ""), header = TRUE, sep = ",")

## EDA rapide =====
# Vérification de la distribution des variables et abscence d'outliers avant la transformation des variables

ExpReport(data, op_file="data_export/sample_fullfiltered_smartEDA.html")


## TRANSFORMATION DE VARIABLES =====

# Remove outlier abstact length, log transform number of authors, remove version ----

df_filtered <- data %>% 
  filter(abstractLength != max(abstractLength)) %>%
  mutate (numberAuthorsLog = log(numberAuthors)) %>%
  mutate (cit1yearLog = log(cit1year)) %>%
  select (-version)

# Publistatus ----

## Reoarder levels publiStatus
order_publiStatus=c('NP', 'year1', 'year2', 'year2+')
df_filtered <- df_filtered %>%
  mutate(!!'publiStatus' := factor(!!sym('publiStatus'), levels = order_publiStatus))

df_transform <- df_filtered %>%
  mutate(publiStatus_binary = case_when(
    publiStatus %in% c('year2+', 'year2', 'NP') ~ 'NotPublished_year1',
    publiStatus == "year1" ~ "Published_year1",
    TRUE  ~ publiStatus))

# Date to month ---

df_transform <- df_transform %>%
  mutate(month_upload = month(ymd(date), label = TRUE, abbr = TRUE))  

# Categories ----
freq_categories <- df_transform %>%
  count(category) %>%
  arrange(desc(n))

print(freq_categories)


### Recoding categories 

df_recode <- df_transform %>%
  mutate(category_grouped = case_when(
    # Neuroscience & Cognition
    category %in% c("neuroscience", "neurology", "psychiatry and clinical psychology", "animal behavior and cognition") ~ "Neuro",
    
    # Infectious Diseases
    category %in% c("infectious diseases", "hiv aids") ~ "InfectDis",
    
    # Genetics, Genomics
    category %in% c("genomics", "genetics", "genetic and genomic medicine") ~ "GenetGenom",
    
    # Evolution and ecology, zoology
    category %in% c("ecology", "zoology") ~ "EcoZoo",
    
    # Evolution 
    category %in% c("evolutionary biology","paleontology") ~ "PaleoEvol",
    
    # Molecular & Cellular Biology
    category %in% c("cell biology", "molecular biology", "biochemistry") ~ "MolCellBio",
    
    # Cancer Research
    category %in% c("cancer biology", "oncology") ~ "CancerOnco",
    
    # Immunology
    category %in% c("immunology", "allergy and immunology") ~ "Immuno",
    
    # Public Health & Epidemiology & Ethics
    category %in% c("epidemiology", "public and global health", "health policy", "health economics", "occupational and environmental health", "health systems and quality improvement", "medical education", "scientific communication and education", "medical ethics") ~ "PublicHealthEpidemioEthics",
    
    # Bioinformatics Biology
    category %in% c("bioinformatics", "systems biology", "health informatics") ~ "Bioinfo",
    
    # Systems bio and biophysics
    category %in% c("systems biology", "biophysics") ~ "BioPhySystemsBio",
    
    # Plant & Agricultural Sciences
    category %in% c("plant biology") ~ "PlantBio",
    
    # Physiology & Endocrinology
    category %in% c("physiology", "endocrinology", "nutrition") ~ "PhysioEndocrino",
    
    # Pharmacology & Therapeutics & trials
    category %in% c("pharmacology and toxicology", "pharmacology and therapeutics", "toxicology", "clinical trials") ~ "PharmacoTherapeutics",
  
    # Medical Specialties & Subfields
    category %in% c("nephrology", "gastroenterology", "hematology", "dermatology", "ophthalmology", "otolaryngology", "urology", "pediatrics", "geriatric medicine", "addication medecine", "dentistry and oral medicine", "pediatrics", "geriatric medicine", "rehabilitation medicine and physical therapy", "sports medicine", "obstetrics and gynecology", "sexual and reproductive health", "cardiovascular medicine", "respiratory medicine", "addiction medicine", "intensive care and critical care medicine", "emergency medicine", "pain medicine", "palliative medicine", "primary care research", "nursing", "surgery", "transplantation", "orthopedics", "radiology and imaging", "anesthesia", "rheumatology", "pathology") ~ "OtherMedecineSpecialties",
    
    # Synthetic & Bioengineering
    category %in% c("synthetic biology", "bioengineering") ~ "SyntheticBioengineering",
    
    category %in% c("microbiology") ~ "MicroBio",

    category %in% c("developmental biology") ~ "DevelopmentBio",
    
    # Default: Keep original category if not matched
    TRUE ~ category
    ))

df_recode%>%
  count(category_grouped) %>%
  arrange(desc(n))


## New EDA on transformed data ====

ExpReport(df_recode, op_file="plots/sample_fullfiltered_transformed_smartEDA.html")

## Univariate analysis plots ----

# histogram with density line
histogram_with_density <- function(data, x_var, binwidth = NULL, color = "darkgreen") {
  ggplot(data, aes_string(x = x_var)) +
    geom_histogram(aes(y = ..density..), binwidth = binwidth, color = "grey", fill = color, alpha = 0.6) +
    geom_density(color = "black", size = 0.8) +
    theme_minimal() +
    labs(x = x_var, y = "Densité", title = paste("Distribution de", x_var)) +
    theme(plot.title = element_text(hjust = 0.5))
}

histogram_with_density(df_transform, 'numberAuthors')
histogram_with_density(df_transform, 'numberAuthorsLog')
histogram_with_density(df_transform, 'titleLength')
histogram_with_density(df_transform, 'abstractLength')

# BarPlot categorical variables

bar_plot <- function(data, category) {
  
  data <- data %>%
    mutate(!!category := factor(!!sym(category), 
                                        levels = names(sort(table(!!sym(category)), decreasing = TRUE))))
  
  tot<- nrow(data)
  print(tot)
  
  ggplot(data, aes_string(x = category, fill = category)) +
    geom_bar(position = "stack", color = "black") +
    labs(title = paste("Nombre d'archives par", category),
         x = category,
         y = "Nombre de documents") +
    theme_minimal() +
    theme(
      plot.title = element_text(hjust = 0.5),
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.title = element_blank()  # Remove the legend title
    ) +
    geom_text(stat = "count", aes(label = ..count..), position = position_stack(vjust = 0.5), color = "black")
    #geom_text(stat = "count", aes(label = paste(round((..count../tot*100), digits=1), "%")), position = position_stack(vjust = 0.5), color = "white")
    
}

bar_plot(df_recode, "category_grouped")
bar_plot(df_recode, "server")
bar_plot(df_recode, "month_upload")
bar_plot(data, "version")


# Plot publiStatus
tot<- nrow(df_recode)
colors_published<-  c("NotPublished_year1" = "darkslateblue", "Published_year1" = "aquamarine4")
colors_published_full<-  c("NP" = "darkslateblue", "year1" = "aquamarine3", "year2"= "aquamarine4", "year>2"="darkslategray")

publiStatus_plot<- function(df, cat, palette){
  plot<- ggplot(df, aes_string(x = cat, fill = cat)) +
    geom_bar(position = "stack", color = "black") +
    scale_fill_manual(values = palette) +
    labs(title = paste("Nombre d'archives par", cat),
         x = cat,
         y = "Nombre de documents") +
    theme_minimal() +
    theme(
      plot.title = element_text(hjust = 0.5),
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.title = element_blank()  # Remove the legend title
    ) +
    geom_text(stat = "count", aes(label = paste(round((..count../tot*100), digits=1), "%")), position = position_stack(vjust = 0.5), color = "white")
  
  return (plot)
}

publiStatus_plot(df_recode, 'publiStatus_binary', colors_published)
publiStatus_plot(df_recode, 'publiStatus', colors_published_full)

# Bi-variate analysis =========

## ANALYSE BIVARIEE =====
df_clean<- df_recode %>%
  select (-doi, -BioBert100, -LDA40, -author_corresponding_institution, -date, -category, -numberAuthors, -publiStatus#, -cit1yearlog
          )

df_clean %>%
  select(-category_grouped)%>%
  ggpairs()

## Numerical vs numerical ----

ggcorrmat(
  data = df_recode, ## data from which variable is to be taken
  cor.vars = c(titleLength:abstractLength, numberAuthorsLog, cit1year) ## specifying correlation matrix variables
)

ggscatterstats(
  data = df_recode, 
  x = titleLength,
  y = abstractLength)

ggscatterstats(
  data = df_recode, 
  x = numberAuthorsLog,
  y = cit1year)

## Categorical vs categorical -----
## Test Chi2 pour identifier correlations entre variables qualitatives 
CatVar <- c('category_grouped', 'server', 'publiStatus_binary', 'month_upload')

chi_squared_test <- function(var1, var2, data) {
  table <- table(data[[var1]], data[[var2]])
  test <- chisq.test(table)
  return(test$p.value)
}

chi2_all<- function(df, CatVar) {

  # Tableau pour conserver les p-values
  p_values <- matrix(NA, nrow = length(CatVar), ncol = length(CatVar))
  rownames(p_values) <- CatVar
  colnames(p_values) <- CatVar
  
  # Loop through all pairs of variables and perform the Chi-squared test
  for (i in 1:length(CatVar)) {
    for (j in i:length(CatVar)) {
      if (i != j) {
        p_values[i, j] <- chi_squared_test(CatVar[i], CatVar[j], df)
      }
    }
  }
  return(p_values)
}

chi2_all(df_clean, CatVar) 


# Function to create a mosaic plot with Chi-squared stat automatically
mosaic_plot_with_chi2 <- function(data, var1, var2) {
  contingency_table <- table(data[[var1]], data[[var2]])
  chi2_test <- chisq.test(contingency_table)
  print(chi2_test)
  p_value <- round(chi2_test$p.value, 10)  # Rounded to 4 decimal places

  mosaicplot(contingency_table, 
             main = paste("Mosaic Plot of", var1, "vs", var2, "\nChi2 p-value =", p_value), 
             color = TRUE, 
             shade = TRUE, 
             legend = TRUE)
  
  return(chi2_test)
}

stacked_barplot<- function(df, varX, varY, palette){
  StkPlot <- ggbarstats(
    data = df,
    x = !! sym(varX),
    y = !! sym(varY)
    )
  return(StkPlot)
}

mosaic_plot_with_chi2(df_recode, 'publiStatus_binary', 'month_upload')
mosaic_plot_with_chi2(df_recode, 'publiStatus_binary', 'category_grouped')

stacked_barplot(df_recode, 'publiStatus_binary', 'category_grouped')
stacked_barplot(df_recode, 'publiStatus_binary', 'month_upload')


## Categorical vs numerical ----

boxplot <- function (df, continuousVar, CatVar) {
  ggbetweenstats(
    data  = df,
    x     = !! sym(CatVar),
    y     = !! sym(continuousVar),
    title = paste("Distribution de", continuousVar, "en fonction de", CatVar, sep=' ')
  )
}

numVar=c('titleLength', 'abstractLength', 'numberAuthors', 'cit1year')
catVar=c('category_grouped', 'month_upload', 'server', 'publiStatus_binary')

# Loop to generate boxplots for each combination of numVar and catVar
for (continuousVar in numVar) {
  for (CatVar in catVar) {
    print(boxplot(df_recode, continuousVar, CatVar))
  }
}

kruskal.test(formula = publiStatus_binary ~ numberAuthors, data = df_recode)
boxplot(df_recode, 'numberAuthors', 'publiStatus_binary')

kruskal.test(formula = publiStatus_binary ~ titleLength, data = df_recode)
kruskal.test(formula = publiStatus_binary ~ abstractLength, data = df_recode)

kruskal.test(formula = month_upload ~ numberAuthors, data = df_recode)
kruskal.test(formula = month_upload ~ titleLength, data = df_recode)
kruskal.test(formula = month_upload ~ abstractLength, data = df_recode)
kruskal.test(formula = month_upload ~ cit1year, data = df_recode)

kruskal.test(formula = server ~ numberAuthors, data = df_recode)
kruskal.test(formula = server ~ titleLength, data = df_recode)
kruskal.test(formula = server ~ abstractLength, data = df_recode)
kruskal.test(formula = server ~ cit1year, data = df_recode)

kruskal.test(formula = category_grouped ~ numberAuthors, data = df_recode)
kruskal.test(formula = category_grouped ~ titleLength, data = df_recode)
kruskal.test(formula = category_grouped ~ abstractLength, data = df_recode)
kruskal.test(formula = category_grouped ~ cit1year, data = df_recode)


## Multivariate analysis ====

# AFDM --


res.afdm<- FAMD (df_clean, ncp = 5, sup.var = 4, ind.sup = NULL, graph = TRUE)

eig.val <- get_eigenvalue(res.afdm)
head(eig.val)

fviz_contrib(res.afdm, "var", axes = 1)
fviz_contrib(res.afdm, "var", axes = 2)
fviz_contrib(res.afdm, "var", axes = 3)

fviz_mfa_ind(res.afdm, 
             habillage = "server",
             palette = c("#00AFBB", "#E7B800"),
             addEllipses = TRUE, ellipse.type = "confidence", 
             repel = TRUE,
             label='none') 


fviz_mfa_ind(res.afdm, 
             habillage = "publiStatus_binary", 
             palette = c("darkslateblue", "aquamarine4"),
             addEllipses = TRUE, ellipse.type = "confidence", 
             repel = TRUE,
             label='none')


# Plots for report =======

## Bar plot proportion of published

# Compute the proportion of 'published' for each category
df_propPublished <- df_recode %>%
  group_by(category_grouped) %>%
  summarise(proportion_published = mean(publiStatus_binary == "Published_year1")) %>%
  arrange(desc(proportion_published))  # Sort in descending order

palette_cat<- paletteer_d("ggthemes::Tableau_20")[1:18]

### PLots side to side
 # For side-by-side plots

# Compute ordering based on count
category_order <- df_recode %>%
  count(category_grouped, sort = TRUE) %>%
  pull(category_grouped)

# Convert to factor with the new order
df_recode$category_grouped <- factor(df_recode$category_grouped, levels = rev(category_order))
df_propPublished$category_grouped <- factor(df_propPublished$category_grouped, levels = rev(category_order))


# PLot1 - Count of Articles per Category 
plot_count <- ggplot(df_recode, aes(x = category_grouped, fill=category_grouped)) +
  geom_bar(color = "black") +
  labs(
    #title = "Count of Articles per Category",
       x = "",
       y = "Nombre archives (BioRxiv ou MedRxiv)") +
  theme_minimal() +
  scale_fill_manual(values = palette_cat, guide = "none") +  
  theme(
    plot.title = element_text(hjust = 0.5),
    axis.text.x = element_text(hjust = 0.5))+
  geom_text(stat = "count", aes(label = ..count..), hjust = 1, vjust = 0.5, color = "black", size=3.5)+
coord_flip()

# PLot2 - Proportion of Published Articles 
plot_proportion <- ggplot(df_propPublished, aes(x = category_grouped, y = proportion_published, fill=category_grouped)) +
  geom_bar(stat = "identity", color = "black") +
  labs(
    #title = "Proportion of Published Articles",
       x = "",
       y = "Proportion publiées la première année (%)") +
  theme_minimal() +
  scale_fill_manual(values = palette_cat, guide='none') +
  theme(
    plot.title = element_text(hjust = 0.5),
    axis.text.x = element_text(hjust = 0.5),
    axis.text.y = element_blank())+
  geom_text(aes(label = scales::percent(proportion_published, accuracy = 0.1)), 
            hjust=-0.2, size = 3.5) +
  coord_flip()+
  ylim(0, 1)

# PLot 3 - Boxplot with Scatter Points for 'cit1year'
plot_boxscatter <- ggplot(df_recode, aes(x = category_grouped, y = cit1year, fill=category_grouped)) +
  geom_jitter(color = "black", size = 2, width = 0.2, alpha=0.1) +  # Scatter points
  geom_boxplot(outlier.shape = NA, color = "black", width = 0.8, alpha=0.8) +  # Boxplot
  labs(
    #title = "Boxplot with Scatter for cit1year",
       x = "",
       y = "Nombre de citations dans la première année de publication") +
  theme_minimal() +
  scale_y_log10() + 
  scale_fill_manual(values = palette_cat, guide='none') +
  theme(
    plot.title = element_text(hjust = 0.5),
    axis.text.y = element_blank())+
  stat_summary(fun = "mean", geom = "text", aes(label = round(..y.., 1)), 
               color = "black", size = 3.5, vjust = 0.5, hjust=-0.5) + 
  stat_summary(fun = "mean", geom = "point", shape = 21, color = "black", fill='white', size = 2) +  
  coord_flip()


# 3 plots combined
plot_count + plot_proportion+plot_boxscatter
