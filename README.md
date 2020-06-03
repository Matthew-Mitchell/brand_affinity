A python implementation of <a href="http://www.cs.iit.edu/~culotta/pubs/culotta16mining.pdf"> Aaron Culotta and Jennifer Cutler Mining Brand Perceptions from Twitter Social Networks algorithm. </a>

Their algorithm calculates a proxy metric for estimating the public’s perception of brand traits such as environmentally friendly or luxury. To do this, they leverage Twitter lists—user curated collections of Twitter handles. For example, I might create a Twitter list ‘Politics’ to keep up to date with political entities of interest. Culotta and Cutler’s algorithm investigates the overlap of a brand’s Twitter followers to all of the followers of handlers from lists pertaining to the given category of interest. To demonstrate, imagine trying to determine perceptions of Eco friendliness between Exon and BP. The user would first specify keywords such as ‘green energy’ and ‘eco-friendly’ and search for all matching lists. These lists would then mined for Twitter handles (they specify that they only include handles that occur on at least 2 matching lists) and in turn the followers of these twitter handles are recorded. Finally, these followers are then compared to the followers of the brand itself and the final metric is calculated using the Jaccard Index.