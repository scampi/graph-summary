/*******************************************************************************
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 *******************************************************************************/

package org.sindice.core.analytics.cascading.statistics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.yaml.snakeyaml.Yaml;

public class DifferentialTablesCountersExportCLI extends AbstractExportCLI{
  
  private Yaml         yaml;
  private PrintWriter  resultsPerJob;
  private PrintWriter  figure;
  private PrintWriter  meanAndVariance;
  private ArrayList<Iterator<Object>> iteratorCollection = new ArrayList<Iterator<Object>>();
  private ArrayList<BufferedReader> bufferList = new ArrayList<BufferedReader>();
  private ArrayList<String> firstEntryList;
  private TreeMap<String, Object> treeMap = new TreeMap<String, Object>();
  private Integer[] lengthOfCounterGroup;
  private ArrayList<String> id;
 
  private ArrayList<ArrayList<String>> listOfCounters;
  private Map<String, Object> countersByFlowName;
  private Map<String, Object> countersByCounterGroups;
  private Map<String, Object> countersGroupFlowName;
  private Map<String, Double> mean = new TreeMap<String, Double>();
  private ArrayList<String> directoriesPath;
  private String[] directories;
  private ArrayList<String> numberOfRun;
  private Map<String, Object> secondMapList;
  private Long[] sumOfTimes;
  private ArrayList<String> algorithmNames;

  private Map<String, String> meanStore = new TreeMap<String, String>();
  private Map<String, Double> secondVarianceStore;
  private Map<String, Object> firstVarianceStore = new TreeMap<String, Object>();

  @Override
  protected void doRun(String input) throws IOException {
    this.numberOfRun = new ArrayList<String>();
    numberOfRuns(input);  
    Collections.sort(this.numberOfRun);
    meanAndVariance = new PrintWriter(new BufferedWriter(new FileWriter(
        new File(input + "/meanAndVariance.tex"))));
    for(int l = 0 ; l < this.numberOfRun.size() ; l ++) {
      this.id = new ArrayList<String>();
      this.directoriesPath = new ArrayList<String>();
      String runNumber =  this.numberOfRun.get(l);
      this.algorithmNames = new ArrayList<String>();
      this.secondVarianceStore = new TreeMap<String, Double>();
      this.countersByFlowName = new TreeMap<String, Object>();
      this.countersGroupFlowName = new TreeMap<String, Object>();
      this.secondMapList = new TreeMap<String, Object>();

      listDirectoriesFiles(input, runNumber);
      listSubDirectories(input, runNumber);

      this.sumOfTimes = new Long[this.directoriesPath.size()];
      for(int i = 0 ; i < this.sumOfTimes.length ; i++ ){
        this.sumOfTimes[i] = new Long(0);
      }
      
      figure = new PrintWriter(new BufferedWriter(new FileWriter(
          new File(input + "/computationTimes_" + runNumber + ".tex"))));
      int p = 0;
      for(int j = 0 ; j < this.directories.length; j++){
        
        resultsPerJob = new PrintWriter(new BufferedWriter(new FileWriter(
            new File(input + "/overviewDifferentialPerJob_"+ this.directories[j]+ "-" + runNumber + ".tex"))));
     
        writeDocumentHeader(resultsPerJob);
        
        this.iteratorCollection.clear();
        this.bufferList.clear();
        BufferedReader buffJob = null;
        for(int i = 0; i < this.directoriesPath.size() ; i++){
          Collections.sort(this.directoriesPath);
          Collections.sort(this.algorithmNames);
          String types = "TYPES";
          int length = types.length();
          for(int k = 0 ; k < this.algorithmNames.size() ; k++ ){
            if(this.algorithmNames.get(k).contains(types) && this.algorithmNames.get(k).length() == length){
              this.algorithmNames.remove(this.algorithmNames.get(k));
              this.algorithmNames.add(0, types);
            }
            if(this.directoriesPath.get(k).contains("TYPES-")){
              String typesPath = this.directoriesPath.get(k);
              this.directoriesPath.remove(k);
              this.directoriesPath.add(0, typesPath);
            }
          }
          File yamlFilesPerJob = new File(this.directoriesPath.get(i) + "/" + this.directories[j] + "/overview.yaml");
          yaml = new Yaml();
          buffJob = new BufferedReader(new FileReader(yamlFilesPerJob));
          this.iteratorCollection.add(yaml.loadAll(buffJob).iterator());
          this.bufferList.add(buffJob);
        }

        Object[] iteratorCollectionTable = this.iteratorCollection.toArray();

        //If one of the iterator has no more elements we stop, 
        //that means there is not the same number of flows everywhere.
        Boolean hasNext = true;

        while(hasNext){
          final Map<String, Object> mapTypes = (Map<String, Object>)((Iterator<Object>) 
              this.iteratorCollection.get(0)).next();
          final TreeMap<String, Object> firstTreeMapTypes = new TreeMap<String, Object>();
          firstTreeMapTypes.putAll(mapTypes);
          
//          this.iteratorCollection.remove(0);
          //In every Map we have <String, <String, Object>>
          ArrayList<TreeMap<String, Object>> firstTreeMapList = new ArrayList<TreeMap<String, Object>>();
//          for(Iterator<Object> iterator : this.iteratorCollection){
//            Map<String, Object> map = (Map<String, Object>) iterator.next();
//            TreeMap<String, Object> treeMap = new TreeMap<String, Object>();
//            treeMap.putAll(map);
//            firstTreeMapList.add(treeMap);
//          }
          
          for(int k = 1 ;  k < this.iteratorCollection.size() ; k++){
            Map<String, Object> map = (Map<String, Object>) ((Iterator<Object>) iteratorCollectionTable[k]).next();
            TreeMap<String, Object> treeMap = new TreeMap<String, Object>();
            treeMap.putAll(map);
            firstTreeMapList.add(treeMap);
          }

          this.listOfCounters = new ArrayList<ArrayList<String>>();

          //We fill the list of Keys with the counters names from the types algorithm
          extractFirstKeys(firstTreeMapTypes);
          //We compare to see if some counters names from the other algorithms are missing in this same arraylist
          for(TreeMap<String,Object> treeMap : firstTreeMapList){
            extractKeys(treeMap);
          }
          
          this.countersByCounterGroups = new TreeMap<String, Object>();
          for(int k = 0 ; k < this.firstEntryList.size(); k++){
              this.countersByCounterGroups.put(this.firstEntryList.get(k), this.listOfCounters.get(k));   
          }
          this.countersGroupFlowName.put(this.id.get(p), this.firstEntryList);
          this.countersByFlowName.put(this.id.get(p) , this.countersByCounterGroups);
        
          
          //Ordering the key values
          lengthOfCounterGroup= new Integer[this.listOfCounters.size()];

          for(int m = 0 ; m < this.listOfCounters.size() ; m ++){
            ArrayList<String> arrayOfKeys =  new ArrayList<String>();
            arrayOfKeys = this.listOfCounters.get(m);
            Collections.sort(arrayOfKeys);
            for(int n = 0; n < arrayOfKeys.size(); n++){
              this.lengthOfCounterGroup[m] = n+1;
            }  
          } 

          Iterator<String> countersGroupIterator = (Iterator<String>) this.firstEntryList.iterator();

          writeTableHeader(resultsPerJob);

          for(Iterator<Object> iterator : this.iteratorCollection){
            if(!iterator.hasNext()){
              hasNext = false;
              break;
            }
          }
          
          for(int i = 0 ; i < this.lengthOfCounterGroup.length; i++){
            String counterGroupKey = countersGroupIterator.next();
            resultsPerJob.print("\\hline \\multirow{" + this.lengthOfCounterGroup[i] + "}{*}{" +
                counterGroupKey.replace("$", "\\$") + "}");

            ArrayList<String> groupOfCounters = this.listOfCounters.get(i); 

            Map<String,Object> firstValueTypes = (Map<String, Object>) firstTreeMapTypes.get(counterGroupKey);
            ArrayList<Map<String,Object>> mapValueList = new ArrayList<Map<String,Object>>();
            for(TreeMap<String, Object> treeMap : firstTreeMapList){
              Map<String,Object> map = (Map<String, Object>) treeMap.get(counterGroupKey);
              mapValueList.add(map);
            }
            
            for(int k = 0 ; k < this.lengthOfCounterGroup[i]; k++){
              String counterName = groupOfCounters.get(k);
              resultsPerJob.print(" & " + counterName.replace("_", "\\_") + " & ");

              if(counterName.equals("CPU_MILLISECONDS")){
                mapValueList.add(0, firstValueTypes);
                int n = 0;
                for(Map<String, Object> map : mapValueList){
                  if(this.id.get(p).equals("dictionary") || this.id.get(p).equals("relations-graph")
                      || this.id.get(p).equals("get-cluste...aph-Summary")){
                    break;
                  } else {
                    this.secondMapList.put(this.id.get(p) + "_" + this.algorithmNames.get(n),
                        map.get("CPU_MILLISECONDS"));
                    n++;
                  }
                }
                mapValueList.remove(firstValueTypes);
              }

              String typeValue = checkData(counterName, firstValueTypes);
              ArrayList<String> values = new ArrayList<String>();
              values.add(typeValue);
              ArrayList<String> percentageValues = new ArrayList<String>();
              for(Map<String, Object> map : mapValueList){
                String value = checkData(counterName, map);
                values.add(value);
                String percentageValue = checkPercentage(typeValue, value);
                percentageValues.add(percentageValue);
              }
             
             this.meanStore.clear();
             for(int m = 0 ; m < values.size() ; m++) {
               this.meanStore.put(this.algorithmNames.get(m), values.get(m));
             }
             storeMean(this.meanStore, this.id.get(p), counterGroupKey, counterName);
             storeVariance(this.meanStore, this.numberOfRun.get(l), this.id.get(p), counterGroupKey, counterName);


              if(!typeValue.equals("No value")){
                typeValue = "\\numprint{" + typeValue + "}";
              }
              
              if(counterName.equals("CPU_MILLISECONDS")){
                mapValueList.add(0, firstValueTypes);
                int n = 0;
                for(Map<String, Object> map : mapValueList){
                  if(this.id.get(p).equals("dictionary") || this.id.get(p).equals("relations-graph")
                      || this.id.get(p).equals("get-cluste...aph-Summary")){
                    break;
                  } else {
                    this.sumOfTimes[n] += Long.valueOf(String.valueOf(map.get("CPU_MILLISECONDS")));
                    n++;
                  }
                }
                mapValueList.remove(firstValueTypes);
              }

   
              resultsPerJob.print(typeValue + " & ");
              for(int m = 0 ; m < percentageValues.size() ; m ++){
                if(m == percentageValues.size() - 1){
                  resultsPerJob.println(percentageValues.get(m) + " \\\\");
                } else {
                  resultsPerJob.print(percentageValues.get(m) + " & ");
                }
              }
              resultsPerJob.flush();
            }
          }
          closeTable(resultsPerJob, this.id.get(p));
          p++;
        }
        for(Reader reader : this.bufferList){
          reader.close();
        }
        closeDocument(resultsPerJob);
        resultsPerJob.close();
      }
      writeFigureDocHeader(figure);
      writeFigureHeader(figure);
      double normalization = 0;
      double value = 0;
      ArrayList<String> secondId = this.id;
      secondId.remove("relations-graph");
      secondId.remove("dictionary");
      secondId.remove("get-cluste...aph-Summary");
      for(String id : secondId){
        figure.print("\\addplot coordinates { ");
        String key = "";
        int m = 0;
        for(String algorithmName : this.algorithmNames){
          key = id + "_" + algorithmName;
          value = ((Integer) this.secondMapList.get(key)).doubleValue();
          normalization = value/this.sumOfTimes[m];
          figure.print("(" + this.algorithmNames.get(m).replace("_", "") +  "," + normalization + ")");
          figure.flush();
          m++;
        }
        figure.println("};");
      }
      figure.print("\\legend{");
      for(int i = 0 ; i < secondId.size() ; i++){
        if(i == secondId.size() -1){
          figure.print(secondId.get(i).replace("-", "").replace("...", ""));
        } else {
          figure.print(secondId.get(i).replace("-", "").replace("...", "") + ",");
        }
      }
      figure.println("}");
      closeFigure(figure);
      closeDocument(figure);
      figure.close();

      this.firstVarianceStore.put(this.numberOfRun.get(l), this.secondVarianceStore);
    }
    String key = "";
    writeDocumentHeader(meanAndVariance);
    for(String flowName : this.id){
      writeTableHeaderMV(meanAndVariance);
      Map<String, Object> counterGroups = (Map<String, Object>) this.countersByFlowName.get(flowName);
      ArrayList<String> counterGroupNames = (ArrayList<String>) this.countersGroupFlowName.get(flowName);
      for(String counterGroupName : counterGroupNames){
        ArrayList<String> counterNames = (ArrayList<String>) counterGroups.get(counterGroupName);
        meanAndVariance.print("\\hline \\multirow{" + counterNames.size() + "}{*}{" +
            counterGroupName.replace("$", "\\$") + "}");
        for(String counterName : counterNames){
          meanAndVariance.print(" & " + counterName.replace("_", "\\_") + " & ");
          ArrayList<String> meanAndVarianceValues = new ArrayList<String>();
          ArrayList<Double> varianceValues = new ArrayList<Double>();
          ArrayList<Double> meanValues = new ArrayList<Double>();
          String meanAndVarianceValue = "";
          for(String algoritmName : this.algorithmNames){
            key = algoritmName + "_" + flowName + "_" + counterGroupName + "_" + counterName;
            Double valueMean = new Double(0);
            Double variance = new Double(0);
            Double[] values = new Double[this.numberOfRun.size()];
            Double mean = new Double(0);
            int i = 0;
            for(String runNumber : this.numberOfRun){
              Map<String, Double> varianceStore = (Map<String, Double>) this.firstVarianceStore.get(runNumber);
              if(varianceStore.get(key) == null){
                values[i] = Double.valueOf(0);
              } else {
                values[i] = varianceStore.get(key);
              }
              i++;
            }
            if(this.mean.get(algoritmName + "_" + flowName + "_" + counterGroupName + "_" + counterName) == null){
              valueMean = Double.valueOf(0);
            } else {
              valueMean = this.mean.get(algoritmName + "_" + flowName + "_" + counterGroupName + "_" + counterName);
            }
            mean = valueMean/this.numberOfRun.size();
            mean = (Math.round(mean*100.0)/100.0);
            for(int j = 0 ; j < values.length ; j++){
              variance += Math.pow(values[j]- mean, 2);
            }
            variance = variance/this.numberOfRun.size();
            variance = Math.sqrt(variance);
            variance = (Math.round(variance*100.0)/100.0);
            meanAndVarianceValue = "\\numprint{" + new BigDecimal(String.valueOf(mean)).toPlainString() + "}" + 
                " (\\numprint{"+ new BigDecimal(String.valueOf(variance)).toPlainString() + "})";
            meanAndVarianceValues.add(meanAndVarianceValue);
            meanValues.add(mean);
            varianceValues.add(variance);
          }
          Double stdDevBase = varianceValues.get(0);
          Double meanBase = meanValues.get(0);
          ArrayList<String> pctDiff = new ArrayList<String>();
          pctDiff.add(0, "");
          for(int l = 1 ; l < meanValues.size() ; l ++){
            pctDiff.add(computePctDiff(meanBase, stdDevBase, meanValues.get(l), varianceValues.get(l)));
          }
          for(int m = 0 ; m < meanAndVarianceValues.size() ; m++){
            if(m == meanAndVarianceValues.size() - 1){
              meanAndVariance.println(meanAndVarianceValues.get(m) + " & " + pctDiff.get(m) + " \\\\");
            } else if(m == 0) {
              meanAndVariance.print(meanAndVarianceValues.get(m) + " & ");
            } else {
              meanAndVariance.print(meanAndVarianceValues.get(m) + " & " + pctDiff.get(m) + " & ");
            }
            meanAndVariance.flush();
          }
        }
      }
      closeTable(meanAndVariance, flowName);
    }
    closeDocument(meanAndVariance);
    meanAndVariance.close();
  }
  
  public String computePctDiff(double qpsBase, double qpsStdDevBase,
      double qpsCmp, double qpsStdDevCmp) {
    final double qpsBaseBest = qpsBase + qpsStdDevBase;
    final double qpsBaseWorst = qpsBase - qpsStdDevBase;
    final double qpsCmpBest = qpsCmp + qpsStdDevCmp;
    final double qpsCmpWorst = qpsCmp - qpsStdDevCmp;
    final int psBest;
    final int psWorst;
    String result ="";
    if(qpsBaseWorst == 0.0 || 
        (qpsBaseBest == qpsCmpBest && qpsBaseWorst == qpsCmpWorst)) {
      psBest = psWorst = 0;
    } else {
      psBest = (int) (100.0 * (qpsCmpBest - qpsBaseWorst)/qpsBaseWorst);
      psWorst = (int) (100.0 * (qpsCmpWorst - qpsBaseBest)/qpsBaseBest);
    }
    if(psWorst < 0){
      result += "\\color{green}{" + psWorst + "\\%}";
    } else if(psWorst >0){
      result += "\\color{red}{" + psWorst + "\\%}";
    } else {
      result += "\\color{blue}{" + psWorst + "\\%}";
    }
    result += " / ";
    if(psBest < 0){
      result += "\\color{green}{" + psBest + "\\%}";
    } else if(psBest >0){
      result += "\\color{red}{" + psBest + "\\%}";
    } else {
      result += "\\color{blue}{" + psBest + "\\%}";
    }
    return result;
  }
  
  public void storeMean(Map<String, String> map, 
      String flowName, String counterGroupName, String counterName){
    String key = "";
    String value = "";
    Double mean = new Double(0);
    for(int i = 0 ; i < map.size() ; i++){
      key = this.algorithmNames.get(i) + "_" + flowName + "_" + counterGroupName + "_" + counterName;
      value = map.get(this.algorithmNames.get(i));
      if(!value.equals("No value")){
        if(this.mean.get(key) == null) {
          this.mean.put(key, Double.valueOf(value));
        } else {
          mean = this.mean.get(key);
          mean += Double.valueOf(value);
          this.mean.remove(key);
          this.mean.put(key, mean);
        }
      } else {
        if(this.mean.get(key) == null) {
          this.mean.put(key, Double.valueOf(0));
        } else {
          mean = this.mean.get(key);
          mean += Double.valueOf(0);
          this.mean.remove(key);
          this.mean.put(key, mean);
        }
      }
    }
  }
  
  public void storeVariance(Map<String, String> map, String runName, String flowName,
    String counterGroupName, String counterName){
    String firstKey = "";
    String value = "";
    for(int i = 0 ; i < map.size() ; i++){
      firstKey = this.algorithmNames.get(i) + "_" + flowName + "_" + counterGroupName + "_" + counterName;
      value = map.get(this.algorithmNames.get(i));
      if(value.equals("No value")){
        this.secondVarianceStore.put(firstKey, Double.valueOf(0));
      } else {
        this.secondVarianceStore.put(firstKey, Double.valueOf(value));
      }
    }
  }
  
  public void numberOfRuns(String inputPath){
    File dir = new File(inputPath);
    File[] listAlgorithmDirectories = dir.listFiles();
    
    for(int i = 0 ; i < listAlgorithmDirectories.length ; i++){
      if(listAlgorithmDirectories[i].isDirectory()){
        String path = listAlgorithmDirectories[i].getAbsolutePath();
        int index = path.lastIndexOf('r');
        this.numberOfRun.add(path.substring(index, path.length()));
      }
    }
    HashSet<String> hs = new HashSet<String>();
    hs.addAll(this.numberOfRun);
    this.numberOfRun.clear();
    this.numberOfRun.addAll(hs);
  }
  
  public void listDirectoriesFiles(String inputPath, String endingWith){
    File dir = new File(inputPath);
    File[] listAlgorithmDirectories = dir.listFiles();
    int endIndex = 0, beginIndex = 0;
    String directoryPath = "";
    for(int i = 0 ; i < listAlgorithmDirectories.length ; i++){
      if(listAlgorithmDirectories[i].isDirectory() && 
          listAlgorithmDirectories[i].getAbsolutePath().endsWith(endingWith)){
        directoryPath = listAlgorithmDirectories[i].getAbsolutePath();  
        beginIndex = directoryPath.lastIndexOf('/') + 1;
        endIndex = directoryPath.indexOf('-', beginIndex);
        this.algorithmNames.add(directoryPath.substring(beginIndex, endIndex));
        this.directoriesPath.add(directoryPath);
      }
    }
  }
  
  public void listSubDirectories(String inputPath, String runNumber){
    for(String algorithmName :this.algorithmNames){
      File subDir = new File(inputPath + "/" + algorithmName + "-" + runNumber);
      File[] listAlgorithmSubDirectories = subDir.listFiles();
      this.directories = new String[listAlgorithmSubDirectories.length];
      for(int i = 0 ; i < listAlgorithmSubDirectories.length ; i++){
        if(listAlgorithmSubDirectories[i].isDirectory()){
          this.directories[i] = listAlgorithmSubDirectories[i].getName();
        }
      }
    }
  }
  
  public String compareValues(String valuesOfTypes, String valuesOfOthers){
    float valuesOfTypesLong = Float.valueOf(valuesOfTypes);
    float valuesOfOthersLong = Float.valueOf(valuesOfOthers);
    float percentage = 0;
    percentage = ((valuesOfOthersLong - valuesOfTypesLong)/valuesOfTypesLong)*100;
    if(Float.isNaN(percentage) || Float.isInfinite(percentage))
      percentage = 0;
    String percentageString ="";
    percentage = (float) (Math.round(percentage*100.0)/100.0);
    if(percentage > 0)
      percentageString = "\\numprint{" + valuesOfOthers + "} (\\color{red}{+" + percentage + "}\\%)";
    else if(percentage < 0)
      percentageString =  "\\numprint{" + valuesOfOthers + "} (\\color{green}{" + percentage + "}\\%)";
    else
      percentageString =  "\\numprint{" + valuesOfOthers + "} (\\color{blue}{" + percentage + "}\\%)";
    return percentageString;
  }
  
  public String checkPercentage(String valuesOfTypes, String valuesOfOthers){
    String percentageFormatted = "";
    if(valuesOfTypes.equals("No value") || valuesOfOthers.equals("No value")){
      if(valuesOfTypes.equals("No value") && !valuesOfOthers.equals("No value")){
      percentageFormatted = "\\numprint{" + valuesOfOthers + "} (None)";
      } else {
        percentageFormatted = "No value (None)";
      }
    } else {
      percentageFormatted = compareValues(valuesOfTypes, valuesOfOthers);
    }
    return percentageFormatted;
  }

  public String checkData(String counterName, Map<String,Object> map){
    String value = "";
    if(map.get(counterName) != null){
        value = String.valueOf(map.get(counterName));
    } else {
      value = "No value";
    }
    return value;
  }
   
  public void extractFirstKeys(Map<String, Object> map){
    this.firstEntryList = new ArrayList<String>();
    for(Entry<String, Object> firstEntry  : map.entrySet()){
      if(firstEntry.getValue() instanceof String) this.id.add(firstEntry.getValue().toString().replace("_", "\\_"));
      else {
        this.firstEntryList.add(firstEntry.getKey());
        this.treeMap.clear();
        this.treeMap.putAll((Map<String, Object>) firstEntry.getValue());
        ArrayList<String> arrayOfKeys = new ArrayList<String>();
        for (Entry<String, Object> secondEntry : this.treeMap.entrySet()) {
          arrayOfKeys.add(secondEntry.getKey());
        }
        this.listOfCounters.add(arrayOfKeys);
      }
    }
  }
  
  public void extractKeys(Map<String, Object> map){
    int k = 0;
    for(Entry<String, Object> firstEntry  : map.entrySet()){
      if(!(firstEntry.getValue() instanceof String)){
        this.treeMap.clear();
        this.treeMap.putAll((Map<String, Object>) firstEntry.getValue());
        ArrayList<String> arrayOfKeys = new ArrayList<String>();
        arrayOfKeys = this.listOfCounters.get(k);
        for (Entry<String, Object> secondEntry : this.treeMap.entrySet()) {
          Boolean newKey = false;
          for(int j = 0 ; j < arrayOfKeys.size() ; j++){
            if(!(arrayOfKeys.get(j).equals(secondEntry.getKey()))){
              newKey = true;
            } else {
              newKey = false;
              break;
            }
          }
          if(newKey.equals(true)){
            arrayOfKeys.add(secondEntry.getKey());
          }
        }
        this.listOfCounters.remove(k);
        this.listOfCounters.add(k, arrayOfKeys);
        k++;
      }  
    }
  }

  public void writeDocumentHeader(PrintWriter p) {
    p.println("\\documentclass{article}");
    p.println("\\usepackage[utf8]{inputenc}");
    p.println("\\usepackage[T1]{fontenc}");
    p.println("\\usepackage{multirow}");
    p.println("\\usepackage{numprint}");
    p.println("\\usepackage{graphicx}");
    p.println("\\usepackage{color}");
    p.println("\\usepackage[usenames,dvipsnames,svgnames,table]{xcolor}");
    p.println("\\npthousandsep{,}");
    p.println("\\usepackage[section] {placeins}");
    p.println("\\begin{document}");
  }

  /**
   * <p>
   * Write the header of every table
   * </p>
   */
  public void writeTableHeader(PrintWriter p) {
    p.println("\\begin{table}");
    p.println("\\resizebox{\\textwidth}{!}{");
    p.println("\\centering");
    p.println("\\begin{tabular}{|l|l|r|r|r|r|}");
    p.println("\\hline \\multicolumn{2}{c|}{} & \\multicolumn{4}{c|}{Comparison between Algorithms} \\\\");
    p.println("\\hline CountersGroup & CountersName & Types & Properties & SingleType & TypesProperties \\\\");
  }
  
  public void writeTableHeaderMV(PrintWriter p) {
    p.println("\\begin{table}");
    p.println("\\resizebox{\\textwidth}{!}{");
    p.println("\\centering");
    p.println("\\begin{tabular}{|l|l|r|r|r|r|r|r|r|}");
    p.println("\\hline \\multicolumn{2}{c|}{} & \\multicolumn{7}{c|}{Comparison between Algorithms} \\\\");
    p.println("\\hline \\multicolumn{2}{c|}{} &  Types &  \\multicolumn{2}{c|}{Properties} " +
    		"&  \\multicolumn{2}{c|}{SingleType} &  \\multicolumn{2}{c|}{TypesProperties} \\\\");
    p.println("\\hline CountersGroup & CountersName & Mean(Standard Deviation) & Mean(Standard Deviation) " +
    		"& PctDiff (Worst/Best) & Mean(Standard Deviation) & PctDiff (Worst/Best) & Mean(Standard Deviation) & PctDiff (Worst/Best) \\\\");
  }

  /**
   * <p>
   * Write the footer of every tables
   * </p>
   */
  public void closeTable(PrintWriter p, String id) {
    p.println("\\hline");
    p.println("\\end{tabular}");
    p.println("}");
    p.println("\\caption{ID : " + id + "}");
    p.println("\\end{table}");
    p.println("\\clearpage");
  }

  /**
   * <p>
   * Write the footer of the Latex file
   * </p>
   */
  public void closeDocument(PrintWriter p) {
    p.println("\\end{document}");
  }
  
  public void writeFigureDocHeader(PrintWriter p){
    p.println("\\documentclass{article}");
    p.println("\\usepackage{pgf}");
    p.println("\\usepackage{tikz}");
    p.println("\\usepackage{pgfplots}");
    p.println("\\pgfplotsset{compat=newest}");
    p.println("\\usepackage{graphicx}");
    p.println("\\begin{document}");
  }
 
  public void writeFigureHeader(PrintWriter p){
    p.println("\\begin{figure}");
    p.println("\\centering");
    p.println("\\resizebox{\\textwidth}{!}{");
    p.println("\\begin{tikzpicture}");
    p.println("\\begin{axis}[");
    p.print("symbolic x coords={");
    for(int i = 0 ; i < this.algorithmNames.size() ; i++){
      if(i == this.algorithmNames.size() - 1){
        p.print(this.algorithmNames.get(i).replace("_", ""));
      } else {
        p.print(this.algorithmNames.get(i).replace("_", "") + ",");
      }  
    }
    p.println("},");
    p.println("ylabel=Usage,");
    p.println("enlargelimits=0.05,");
    p.println("width=2\\textwidth,");
    p.println("legend style={at={(0.5,-0.15)}, anchor=north,legend columns=3},");
    p.println("ybar,");
    p.println("bar width=7pt,");
    p.println("xticklabel style={");
    p.println("inner sep=0pt,");
    p.println("anchor=north east,");
    p.println("rotate=45");
    p.println("},");
    p.print("xtick={");
    for(int i = 0 ; i < this.algorithmNames.size() ; i++){
      if(i == this.algorithmNames.size() - 1){
        p.print(this.algorithmNames.get(i).replace("_", ""));
      } else {
        p.print(this.algorithmNames.get(i).replace("_", "") + ",");
      }  
    }
    p.println("}");
    p.println("]");
  }
  
  public void closeFigure(PrintWriter p){
    p.println("\\end{axis}");
    p.println("\\end{tikzpicture}");
    p.println("}");
    p.println("\\caption{Computation times}");
    p.println("\\end{figure}");
  }

  public static void main(String[] args) throws Exception {
    final DifferentialTablesCountersExportCLI cli = new DifferentialTablesCountersExportCLI();
    cli.run(args);
  }

}
