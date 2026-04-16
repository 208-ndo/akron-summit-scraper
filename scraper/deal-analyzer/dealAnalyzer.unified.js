/**
 * =========================================================================
 * UNIFIED REAL ESTATE DEAL ANALYZER
 * =========================================================================
 * 
 * Single file containing all backend logic.
 * Copy this entire file into your project.
 * 
 * Usage:
 *   const DealAnalyzer = require('./dealAnalyzer.unified');
 *   const analyzer = new DealAnalyzer(property, comps, dealParams);
 *   const result = analyzer.analyze(sellerAsk, rentData);
 * 
 * =========================================================================
 */

// =========================================================================
// 1. COMP ANALYZER - Classifies and validates comps
// =========================================================================

class CompAnalyzer {
  constructor(subjectProperty) {
    this.subject = subjectProperty;
    this.subdivisionRuleEnabled = true;
  }

  classifyComp(comp) {
    const exclusionReasons = [];
    
    if (comp.status?.toLowerCase() !== 'sold') {
      return { classification: 'CONTEXT_ONLY', exclusion_reason: 'Not a sold property', weight: 0 };
    }

    if (comp.property_type !== this.subject.property_type) {
      return { classification: 'EXCLUDED_OUTLIER', exclusion_reason: 'Different property type', weight: 0 };
    }

    const distressedKeywords = ['as-is', 'as is', 'asis', 'fixer', 'handyman special', 'condemned', 
                                 'foreclosure', 'reo', 'sheriff sale', 'bank owned', 'estate sale', 
                                 'needs work', 'tlc', 'investor special'];
    const remarks = (comp.remarks || '').toLowerCase();
    const isDistressed = distressedKeywords.some(kw => remarks.includes(kw));

    if (this.subdivisionRuleEnabled && this.subject.subdivision && comp.subdivision &&
        comp.subdivision !== this.subject.subdivision) {
      exclusionReasons.push('Outside subdivision');
    }

    const sizeVariance = Math.abs(comp.sqft - this.subject.sqft) / this.subject.sqft;
    if (sizeVariance > 0.40) {
      exclusionReasons.push(`Size differs by ${(sizeVariance * 100).toFixed(0)}%`);
    }

    const bathDiff = (comp.baths_full || 0) - (this.subject.baths_full || 0);
    if (bathDiff >= 2) {
      exclusionReasons.push('Materially superior (2+ extra full baths)');
    }

    const saleDate = new Date(comp.sale_date);
    const monthsAgo = (Date.now() - saleDate.getTime()) / (1000 * 60 * 60 * 24 * 30);
    if (monthsAgo > 12) {
      exclusionReasons.push(`Sold ${monthsAgo.toFixed(0)} months ago`);
    }

    if (exclusionReasons.length > 0) {
      return { classification: 'EXCLUDED_OUTLIER', exclusion_reason: exclusionReasons.join('; '), weight: 0 };
    }

    if (isDistressed) {
      return { classification: 'AS_IS_COMP', exclusion_reason: null, weight: this.calculateWeight(comp, 'as_is') };
    }

    const renovatedKeywords = ['renovated', 'updated', 'remodeled', 'new kitchen', 'new bath', 
                               'granite', 'stainless', 'hardwood', 'move-in ready', 'turnkey', 'like new'];
    const isRenovated = renovatedKeywords.some(kw => remarks.includes(kw));
    
    if (isRenovated || comp.condition === 'excellent' || comp.condition === 'good') {
      return { classification: 'ARV_COMP', exclusion_reason: null, weight: this.calculateWeight(comp, 'arv') };
    }

    return { classification: 'CONTEXT_ONLY', exclusion_reason: 'Condition uncertain, not clearly renovated', weight: 0 };
  }

  calculateWeight(comp, compType) {
    let weight = 1.0;
    if (this.subject.subdivision && comp.subdivision === this.subject.subdivision) weight += 0.5;
    
    const sizeVariance = Math.abs(comp.sqft - this.subject.sqft) / this.subject.sqft;
    if (sizeVariance < 0.10) weight += 0.3;
    else if (sizeVariance < 0.20) weight += 0.15;
    
    if (comp.baths_full === this.subject.baths_full) weight += 0.2;
    
    const monthsAgo = (Date.now() - new Date(comp.sale_date).getTime()) / (1000 * 60 * 60 * 24 * 30);
    if (monthsAgo < 3) weight += 0.3;
    else if (monthsAgo < 6) weight += 0.15;
    
    return weight;
  }

  calculateAdjustments(comp) {
    const adjustments = { sqft: 0, bath: 0, garage: 0, lot_size: 0, condition: 0 };
    
    const pricePerSqft = comp.sale_price / comp.sqft;
    const sqftDiff = this.subject.sqft - comp.sqft;
    adjustments.sqft = sqftDiff * (pricePerSqft * 0.8);
    
    const bathDiff = (this.subject.baths_full || 0) - (comp.baths_full || 0);
    if (bathDiff !== 0) adjustments.bath = bathDiff * 8000;
    
    const garageDiff = (this.subject.garage_spaces || 0) - (comp.garage_spaces || 0);
    if (garageDiff !== 0) adjustments.garage = garageDiff * 5000;
    
    if (comp.lot_sqft && this.subject.lot_sqft) {
      const lotDiff = this.subject.lot_sqft - comp.lot_sqft;
      if (Math.abs(lotDiff) > 2000) adjustments.lot_size = lotDiff * 0.5;
    }
    
    return adjustments;
  }

  analyzeComps(comps) {
    const results = { arv_comps: [], as_is_comps: [], excluded_outliers: [], context_only: [] };

    comps.forEach(comp => {
      const classification = this.classifyComp(comp);
      const adjustments = this.calculateAdjustments(comp);
      const totalAdjustment = Object.values(adjustments).reduce((sum, adj) => sum + adj, 0);
      const adjustedPrice = comp.sale_price + totalAdjustment;

      const compResult = {
        comp_id: comp.id || comp.address,
        address: comp.address,
        sale_price: comp.sale_price,
        sqft: comp.sqft,
        classification: classification.classification,
        exclusion_reason: classification.exclusion_reason,
        adjustments,
        adjusted_price: adjustedPrice,
        weight: classification.weight,
        sale_date: comp.sale_date,
        subdivision: comp.subdivision
      };

      switch (classification.classification) {
        case 'ARV_COMP': results.arv_comps.push(compResult); break;
        case 'AS_IS_COMP': results.as_is_comps.push(compResult); break;
        case 'EXCLUDED_OUTLIER': results.excluded_outliers.push(compResult); break;
        case 'CONTEXT_ONLY': results.context_only.push(compResult); break;
      }
    });

    results.arv_comps = this.removeStatisticalOutliers(results.arv_comps, results.excluded_outliers);
    return results;
  }

  removeStatisticalOutliers(arvComps, excludedList) {
    if (arvComps.length < 3) return arvComps;

    const prices = arvComps.map(c => c.adjusted_price).sort((a, b) => a - b);
    const q1 = prices[Math.floor(prices.length * 0.25)];
    const q3 = prices[Math.floor(prices.length * 0.75)];
    const iqr = q3 - q1;
    const lowerBound = q1 - (1.5 * iqr);
    const upperBound = q3 + (1.5 * iqr);

    const filtered = [];
    arvComps.forEach(comp => {
      if (comp.adjusted_price < lowerBound || comp.adjusted_price > upperBound) {
        excludedList.push({
          ...comp,
          classification: 'EXCLUDED_OUTLIER',
          exclusion_reason: `Statistical outlier (${comp.adjusted_price < lowerBound ? 'too low' : 'too high'})`
        });
      } else {
        filtered.push(comp);
      }
    });

    return filtered;
  }
}

// =========================================================================
// 2. ARV CALCULATOR - Calculates comp-supported ARV with validation
// =========================================================================

class ARVCalculator {
  constructor(subjectProperty, comps) {
    this.subject = subjectProperty;
    this.analyzer = new CompAnalyzer(subjectProperty);
    this.classifiedComps = this.analyzer.analyzeComps(comps);
  }

  calculateWeightedValue(arvComps) {
    if (arvComps.length === 0) return null;

    let totalWeightedValue = 0;
    let totalWeight = 0;

    arvComps.forEach(comp => {
      totalWeightedValue += comp.adjusted_price * comp.weight;
      totalWeight += comp.weight;
    });

    const weightedAverage = totalWeightedValue / totalWeight;
    const sortedComps = [...arvComps].sort((a, b) => a.adjusted_price - b.adjusted_price);
    const median = sortedComps[Math.floor(sortedComps.length / 2)].adjusted_price;
    
    if (weightedAverage > median * 1.15) return median;
    return weightedAverage;
  }

  calculateAsIsValue() {
    const asIsComps = this.classifiedComps.as_is_comps;
    if (asIsComps.length === 0) {
      const arvValue = this.calculateWeightedValue(this.classifiedComps.arv_comps);
      if (arvValue) return arvValue * 0.70;
      return null;
    }
    return this.calculateWeightedValue(asIsComps);
  }

  calculateConfidenceScore() {
    const arvCount = this.classifiedComps.arv_comps.length;
    let score = Math.min(arvCount * 25, 70);

    const sameSubdivision = this.classifiedComps.arv_comps.filter(
      c => c.subdivision === this.subject.subdivision
    ).length;
    if (sameSubdivision > 0) score += 10;

    const recentSales = this.classifiedComps.arv_comps.filter(c => {
      const monthsAgo = (Date.now() - new Date(c.sale_date).getTime()) / (1000 * 60 * 60 * 24 * 30);
      return monthsAgo < 6;
    }).length;
    if (recentSales >= 2) score += 10;

    if (arvCount >= 3) {
      const prices = this.classifiedComps.arv_comps.map(c => c.adjusted_price);
      const avg = prices.reduce((sum, p) => sum + p, 0) / prices.length;
      const variance = prices.reduce((sum, p) => sum + Math.pow(p - avg, 2), 0) / prices.length;
      const stdDev = Math.sqrt(variance);
      const coefficientOfVariation = stdDev / avg;
      if (coefficientOfVariation < 0.10) score += 10;
    }

    return Math.min(score, 100);
  }

  calculateCompQualityScore() {
    const arvComps = this.classifiedComps.arv_comps;
    if (arvComps.length === 0) return 0;

    let totalQuality = 0;
    arvComps.forEach(comp => {
      let compQuality = 50;
      if (comp.subdivision === this.subject.subdivision) compQuality += 20;
      
      const sizeVariance = Math.abs(comp.sqft - this.subject.sqft) / this.subject.sqft;
      if (sizeVariance < 0.10) compQuality += 15;
      else if (sizeVariance < 0.20) compQuality += 10;
      
      const monthsAgo = (Date.now() - new Date(comp.sale_date).getTime()) / (1000 * 60 * 60 * 24 * 30);
      if (monthsAgo < 3) compQuality += 15;
      else if (monthsAgo < 6) compQuality += 10;
      
      totalQuality += compQuality;
    });

    return Math.min(totalQuality / arvComps.length, 100);
  }

  calculate(manualARVOverride = null) {
    const arvComps = this.classifiedComps.arv_comps;
    const asIsValue = this.calculateAsIsValue();

    let compSupportedARV = null;
    let arvSource = 'INSUFFICIENT_DATA';
    let status = 'WARNING';

    if (arvComps.length >= 2) {
      compSupportedARV = this.calculateWeightedValue(arvComps);
      arvSource = 'COMP_SUPPORTED';
      status = 'OK';
    } else if (arvComps.length === 1) {
      compSupportedARV = arvComps[0].adjusted_price;
      arvSource = 'COMP_SUPPORTED';
      status = 'LOW_CONFIDENCE';
    }

    let manualOverrideFlag = false;
    let overrideVariancePct = null;
    let warnings = [];

    if (manualARVOverride && compSupportedARV) {
      const variance = (manualARVOverride - compSupportedARV) / compSupportedARV;
      overrideVariancePct = variance * 100;

      if (variance > 0.10) {
        manualOverrideFlag = true;
        warnings.push({
          level: 'ERROR',
          message: `Manual ARV is ${overrideVariancePct.toFixed(1)}% above comp-supported value`,
          recommendation: 'Review comps or reduce ARV estimate'
        });
        status = 'UNSUPPORTED_OVERRIDE';
      } else if (variance > 0.05) {
        manualOverrideFlag = true;
        warnings.push({
          level: 'WARNING',
          message: `Manual ARV is ${overrideVariancePct.toFixed(1)}% above comp-supported value`,
          recommendation: 'Consider using comp-supported ARV for conservative analysis'
        });
      }
    }

    const confidenceScore = this.calculateConfidenceScore();
    const compQualityScore = this.calculateCompQualityScore();

    return {
      as_is_value: asIsValue ? Math.round(asIsValue) : null,
      comp_supported_arv: compSupportedARV ? Math.round(compSupportedARV) : null,
      manual_arv_override: manualARVOverride,
      override_variance_pct: overrideVariancePct,
      arv_source: arvSource,
      status: status,
      confidence_score: Math.round(confidenceScore),
      comp_quality_score: Math.round(compQualityScore),
      arv_comp_count: arvComps.length,
      as_is_comp_count: this.classifiedComps.as_is_comps.length,
      excluded_outlier_count: this.classifiedComps.excluded_outliers.length,
      context_only_count: this.classifiedComps.context_only.length,
      manual_override_flag: manualOverrideFlag,
      warnings: warnings,
      subdivision_rule_passed: this.analyzer.subdivisionRuleEnabled,
      comp_details: {
        arv_comps: arvComps,
        as_is_comps: this.classifiedComps.as_is_comps,
        excluded_outliers: this.classifiedComps.excluded_outliers,
        context_only: this.classifiedComps.context_only
      }
    };
  }

  getInvestorSafeBuyBasis(arvResult) {
    if (arvResult.status === 'UNSUPPORTED_OVERRIDE') {
      return {
        value: arvResult.comp_supported_arv,
        source: 'COMP_SUPPORTED',
        reason: 'Manual override rejected - exceeds comp support by >10%'
      };
    }

    if (arvResult.status === 'LOW_CONFIDENCE' || arvResult.status === 'WARNING') {
      const conservativeValue = arvResult.as_is_value * 1.15;
      return {
        value: Math.round(conservativeValue),
        source: 'AS_IS_UPLIFT',
        reason: 'Insufficient ARV comps - using conservative as-is estimate'
      };
    }

    if (arvResult.comp_supported_arv) {
      return {
        value: arvResult.comp_supported_arv,
        source: 'COMP_SUPPORTED',
        reason: 'Sufficient comp support with good confidence'
      };
    }

    return {
      value: arvResult.as_is_value,
      source: 'AS_IS_ONLY',
      reason: 'No ARV comps available'
    };
  }
}

// =========================================================================
// 3. MAO CALCULATOR - Three-tier MAO with formula transparency
// =========================================================================

class MAOCalculator {
  constructor(dealParams) {
    this.params = {
      rehabCost: dealParams.rehabCost || 0,
      holdingCosts: dealParams.holdingCosts || 0,
      closingCosts: dealParams.closingCosts || 0,
      wholesaleFee: dealParams.wholesaleFee || 0,
      lowballProfit: dealParams.lowballProfit || 0.30,
      targetProfit: dealParams.targetProfit || 0.20,
      maxProfit: dealParams.maxProfit || 0.12,
      buyerCosts: dealParams.buyerCosts || 0.03,
      sellerCosts: dealParams.sellerCosts || 0.06,
      financingCosts: dealParams.financingCosts || 0,
      useManualOverride: dealParams.useManualOverride || false
    };
  }

  calculateMAO(arv, profitMargin) {
    const sellerCostsDollar = arv * this.params.sellerCosts;
    const buyerCostsDollar = arv * this.params.buyerCosts;
    
    const mao = (arv * (1 - profitMargin - this.params.sellerCosts)) 
                - this.params.rehabCost 
                - buyerCostsDollar
                - this.params.holdingCosts
                - this.params.closingCosts
                - this.params.financingCosts;

    return Math.max(0, Math.round(mao));
  }

  buildFormula(arv, profitMargin, maoValue) {
    const parts = [
      `ARV: $${arv.toLocaleString()}`,
      `Profit: ${(profitMargin * 100).toFixed(0)}%`,
      `Seller Costs: ${(this.params.sellerCosts * 100).toFixed(0)}%`,
      `Rehab: $${this.params.rehabCost.toLocaleString()}`,
      `Buyer Costs: ${(this.params.buyerCosts * 100).toFixed(0)}%`,
    ];

    if (this.params.holdingCosts > 0) parts.push(`Holding: $${this.params.holdingCosts.toLocaleString()}`);
    if (this.params.closingCosts > 0) parts.push(`Closing: $${this.params.closingCosts.toLocaleString()}`);
    if (this.params.financingCosts > 0) parts.push(`Financing: $${this.params.financingCosts.toLocaleString()}`);

    return `${parts.join(' | ')} = $${maoValue.toLocaleString()}`;
  }

  calculate(arvResult) {
    let sourceARV;
    let arvType;
    let warnings = [];

    const arvCalc = new ARVCalculator(null, []);
    const buyBasis = arvCalc.getInvestorSafeBuyBasis(arvResult);

    if (this.params.useManualOverride && arvResult.manual_arv_override) {
      sourceARV = arvResult.manual_arv_override;
      arvType = 'MANUAL_OVERRIDE';
      
      if (arvResult.status === 'UNSUPPORTED_OVERRIDE') {
        warnings.push({
          level: 'ERROR',
          message: 'Using unsupported manual ARV override',
          detail: `Manual ARV ($${sourceARV.toLocaleString()}) is ${arvResult.override_variance_pct.toFixed(1)}% above comp-supported value ($${arvResult.comp_supported_arv.toLocaleString()})`,
          recommendation: 'This may result in overpaying. Consider using comp-supported ARV.'
        });
      }
    } else {
      sourceARV = buyBasis.value;
      arvType = buyBasis.source;
      
      if (arvResult.manual_arv_override && arvResult.manual_arv_override > sourceARV) {
        warnings.push({
          level: 'INFO',
          message: 'Manual override ignored',
          detail: `Using comp-supported ARV ($${sourceARV.toLocaleString()}) instead of manual override ($${arvResult.manual_arv_override.toLocaleString()})`,
          recommendation: 'Enable "Use Manual Override" mode if you want to use the higher value.'
        });
      }
    }

    const lowballMAO = this.calculateMAO(sourceARV, this.params.lowballProfit);
    const targetMAO = this.calculateMAO(sourceARV, this.params.targetProfit);
    const maxMAO = this.calculateMAO(sourceARV, this.params.maxProfit);
    const lowballCashOffer = Math.max(0, lowballMAO - this.params.wholesaleFee);
    const targetCashOffer = Math.max(0, targetMAO - this.params.wholesaleFee);
    const maxCashOffer = Math.max(0, maxMAO - this.params.wholesaleFee);

    const lowballFormula = this.buildFormula(sourceARV, this.params.lowballProfit, lowballMAO);
    const targetFormula = this.buildFormula(sourceARV, this.params.targetProfit, targetMAO);
    const maxFormula = this.buildFormula(sourceARV, this.params.maxProfit, maxMAO);

    if (lowballMAO > targetMAO || targetMAO > maxMAO) {
      warnings.push({
        level: 'ERROR',
        message: 'Invalid MAO range',
        detail: 'Lowball must be ≤ Target ≤ Max. Check profit margins.',
        recommendation: 'Adjust profit margin settings.'
      });
    }

    if (maxMAO <= 0) {
      warnings.push({
        level: 'ERROR',
        message: 'Deal not viable',
        detail: 'Even at minimum profit margin, MAO is zero or negative.',
        recommendation: 'Reduce costs or pass on this deal.'
      });
    }

    return {
      lowball_mao: lowballMAO,
      target_mao: targetMAO,
      max_mao: maxMAO,
      lowball_cash_offer: lowballCashOffer,
      target_cash_offer: targetCashOffer,
      max_cash_offer: maxCashOffer,
      source_arv: sourceARV,
      arv_type: arvType,
      buy_basis_reason: buyBasis.reason,
      formulas: { lowball: lowballFormula, target: targetFormula, max: maxFormula },
      parameters: {
        lowball_profit_pct: this.params.lowballProfit * 100,
        target_profit_pct: this.params.targetProfit * 100,
        max_profit_pct: this.params.maxProfit * 100,
        rehab_cost: this.params.rehabCost,
        holding_costs: this.params.holdingCosts,
        closing_costs: this.params.closingCosts,
        wholesale_fee: this.params.wholesaleFee,
        financing_costs: this.params.financingCosts,
        buyer_costs_pct: this.params.buyerCosts * 100,
        seller_costs_pct: this.params.sellerCosts * 100
      },
      warnings: warnings,
      spread: maxMAO - lowballMAO,
      profit_at_max_mao: Math.round(sourceARV * this.params.maxProfit),
      all_in_cost_at_max_mao: Math.round(maxMAO + this.params.rehabCost + this.params.holdingCosts + this.params.closingCosts),
      roi_at_max_mao: maxMAO > 0 ? ((this.params.maxProfit * sourceARV) / (maxMAO + this.params.rehabCost)) * 100 : 0
    };
  }
}

// =========================================================================
// 4. STRATEGY ANALYZER - Compares flip vs rental vs terms
// =========================================================================

class StrategyAnalyzer {
  constructor(propertyData, arvResult, maoResult, marketData = {}) {
    this.property = propertyData;
    this.arv = arvResult;
    this.mao = maoResult;
    this.market = {
      rentEstimate: marketData.rentEstimate || null,
      rentComps: marketData.rentComps || [],
      appreciationRate: marketData.appreciationRate || 0.03,
      inflationRate: marketData.inflationRate || 0.025,
      interestRate: marketData.interestRate || 0.07
    };
  }

  analyzeFlip() {
    const purchasePrice = this.mao.target_mao;
    const rehabCost = this.mao.parameters.rehab_cost;
    const holdingCosts = this.mao.parameters.holding_costs;
    const closingCosts = this.mao.parameters.closing_costs;
    const arv = this.mao.source_arv;
    const sellerCosts = arv * (this.mao.parameters.seller_costs_pct / 100);

    const totalInvested = purchasePrice + rehabCost + holdingCosts + closingCosts;
    const netProceeds = arv - sellerCosts;
    const profit = netProceeds - totalInvested;
    const roi = (profit / totalInvested) * 100;

    let score = 50;
    if (roi >= 30) score += 30;
    else if (roi >= 20) score += 20;
    else if (roi >= 15) score += 10;
    else if (roi < 10) score -= 20;
    score += (this.arv.confidence_score - 50) / 2;

    const pros = [];
    if (roi >= 25) pros.push('Strong profit margin');
    if (this.arv.arv_comp_count >= 3) pros.push('Good ARV comp support');
    if (this.arv.confidence_score >= 80) pros.push('High confidence valuation');

    const cons = [];
    if (roi < 15) cons.push('Low profit margin');
    if (this.arv.confidence_score < 70) cons.push('ARV uncertainty');

    return {
      strategy: 'FLIP',
      net_profit: Math.round(profit),
      roi_pct: roi.toFixed(1),
      total_invested: Math.round(totalInvested),
      time_to_exit_months: 6,
      score: Math.max(0, Math.min(100, Math.round(score))),
      pros: pros.length > 0 ? pros : ['Standard flip opportunity'],
      cons: cons,
      critical_assumption: this.arv.confidence_score < 70 
        ? 'ARV accuracy - low comp confidence could reduce profit by 20%+'
        : 'ARV and holding time - market shift or delayed sale reduces returns'
    };
  }

  analyzeRental(rentData = {}) {
    const purchasePrice = this.mao.target_mao;
    const rehabCost = this.mao.parameters.rehab_cost;
    const totalInvestment = purchasePrice + rehabCost;
    
    const rentEstimate = rentData.monthlyRent || this.market.rentEstimate;
    const rentComps = rentData.rentComps || this.market.rentComps;
    const rentManualOverride = rentData.isManualOverride || false;
    
    if (!rentEstimate) {
      return { strategy: 'RENTAL', viable: false, reason: 'No rent estimate available', score: 0 };
    }

    const rentConfidence = this.calculateRentConfidence(rentComps, rentManualOverride);
    const rentSourceDate = this.getMostRecentRentCompDate(rentComps);

    const monthlyRent = rentEstimate;
    const propertyTax = (this.mao.source_arv * 0.012) / 12;
    const insurance = 100;
    const maintenance = monthlyRent * 0.10;
    const vacancy = monthlyRent * 0.08;
    const propertyManagement = monthlyRent * 0.10;
    
    const monthlyExpenses = propertyTax + insurance + maintenance + vacancy + propertyManagement;
    const monthlyCashFlow = monthlyRent - monthlyExpenses;
    const annualCashFlow = monthlyCashFlow * 12;
    const cashOnCashReturn = (annualCashFlow / totalInvestment) * 100;
    const capRate = (annualCashFlow / this.mao.source_arv) * 100;

    let score = 50;
    if (cashOnCashReturn >= 12) score += 30;
    else if (cashOnCashReturn >= 8) score += 20;
    else if (cashOnCashReturn >= 5) score += 10;
    else if (cashOnCashReturn < 0) score -= 30;
    const avgConfidence = (rentConfidence + this.arv.confidence_score) / 2;
    score += (avgConfidence - 50) / 2;

    const pros = [];
    if (cashOnCashReturn >= 10) pros.push('Strong cash-on-cash return');
    if (monthlyCashFlow >= 200) pros.push('Positive monthly cash flow');

    const cons = [];
    if (cashOnCashReturn < 5) cons.push('Low cash-on-cash return');
    if (rentConfidence < 60) cons.push('Rent estimate has low confidence');

    return {
      strategy: 'RENTAL',
      viable: monthlyCashFlow > 0,
      monthly_rent: Math.round(monthlyRent),
      monthly_cash_flow: Math.round(monthlyCashFlow),
      annual_cash_flow: Math.round(annualCashFlow),
      cash_on_cash_return_pct: cashOnCashReturn.toFixed(1),
      cap_rate_pct: capRate.toFixed(1),
      total_investment: Math.round(totalInvestment),
      score: Math.max(0, Math.min(100, Math.round(score))),
      rent_comp_count: rentComps.length,
      rent_confidence: rentConfidence,
      rent_source_date: rentSourceDate,
      rent_manual_override_flag: rentManualOverride,
      pros: pros.length > 0 ? pros : ['Standard rental opportunity'],
      cons: cons,
      critical_assumption: rentConfidence < 60
        ? 'Rent estimate accuracy - thin rent comp data, 15% miss eliminates cash flow'
        : 'Occupancy rate - extended vacancy or high turnover reduces returns significantly'
    };
  }

  calculateRentConfidence(rentComps, isManualOverride) {
    if (isManualOverride) return 40;
    let score = 50;
    if (rentComps.length >= 3) score += 20;
    else if (rentComps.length >= 2) score += 10;
    else if (rentComps.length === 0) return 0;

    const recentDate = this.getMostRecentRentCompDate(rentComps);
    if (recentDate) {
      const monthsAgo = (Date.now() - new Date(recentDate).getTime()) / (1000 * 60 * 60 * 24 * 30);
      if (monthsAgo <= 3) score += 20;
      else if (monthsAgo <= 6) score += 10;
      else if (monthsAgo > 12) score -= 20;
    }

    return Math.max(0, Math.min(100, score));
  }

  getMostRecentRentCompDate(rentComps) {
    if (!rentComps || rentComps.length === 0) return null;
    const dates = rentComps.map(c => c.date || c.lease_date).filter(Boolean);
    if (dates.length === 0) return null;
    return dates.sort((a, b) => new Date(b) - new Date(a))[0];
  }

  analyzeTerms(sellerAsk) {
    if (!sellerAsk) {
      return { strategy: 'TERMS', viable: false, reason: 'Seller ask not provided' };
    }

    const maxCashOffer = this.mao.max_mao;
    const gap = sellerAsk - maxCashOffer;
    
    if (gap <= 0) {
      return { strategy: 'TERMS', viable: false, reason: 'Cash offer already acceptable, terms not needed', score: 0 };
    }

    const downPayment = maxCashOffer;
    const sellerCarry = gap;
    const totalPrice = sellerAsk;
    const interestRate = 0.06;
    const termYears = 5;

    const monthlyRate = interestRate / 12;
    const numPayments = termYears * 12;
    const monthlyPayment = sellerCarry * (monthlyRate * Math.pow(1 + monthlyRate, numPayments)) / 
                          (Math.pow(1 + monthlyRate, numPayments) - 1);
    
    const rentEstimate = this.market.rentEstimate;
    const viable = rentEstimate && (rentEstimate * 0.5 > monthlyPayment);

    let score = viable ? 50 : 20;
    const gapPct = (gap / maxCashOffer) * 100;
    if (gapPct <= 15) score += 20;
    else if (gapPct <= 25) score += 10;
    else if (gapPct > 40) score -= 20;
    score += (this.arv.confidence_score - 50) / 2;

    return {
      strategy: 'TERMS',
      viable: viable,
      total_price: Math.round(totalPrice),
      down_payment: Math.round(downPayment),
      seller_carry: Math.round(sellerCarry),
      interest_rate_pct: (interestRate * 100).toFixed(1),
      monthly_payment: Math.round(monthlyPayment),
      term_years: termYears,
      gap_bridged: Math.round(gap),
      score: Math.max(0, Math.min(100, Math.round(score))),
      pros: gap < 15000 ? ['Small gap to bridge', 'No traditional financing needed'] : ['No traditional financing needed'],
      cons: rentEstimate && monthlyPayment > rentEstimate * 0.5 ? ['Payment high relative to potential rent', 'Requires seller to carry note'] : ['Requires seller to carry note'],
      critical_assumption: 'Seller willingness to carry paper - most sellers want all cash'
    };
  }

  compareStrategies(sellerAsk, rentData = {}) {
    const flip = this.analyzeFlip();
    const rental = this.analyzeRental(rentData);
    const terms = this.analyzeTerms(sellerAsk);

    const strategies = [flip, rental, terms].filter(s => s.viable !== false);
    strategies.sort((a, b) => b.score - a.score);
    const recommended = strategies[0] || flip;

    const comparison = {
      winner: recommended.strategy,
      winner_reason: this.getWinnerReason(recommended),
      flip_outcome: this.getStrategyOutcome('flip', flip, recommended),
      rental_outcome: this.getStrategyOutcome('rental', rental, recommended),
      terms_outcome: this.getStrategyOutcome('terms', terms, recommended),
      critical_assumption: recommended.critical_assumption,
      decision_driver: this.getDecisionDriver(flip, rental, terms, recommended)
    };

    return {
      recommended_strategy: recommended.strategy,
      recommended_score: recommended.score,
      strategies: { flip, rental, terms },
      comparison,
      flip_roi: flip.roi_pct,
      rental_cash_flow: rental.monthly_cash_flow || 0,
      terms_viable: terms.viable || false
    };
  }

  getWinnerReason(strategy) {
    switch(strategy.strategy) {
      case 'FLIP': return `Highest ROI (${strategy.roi_pct}%) with ${strategy.time_to_exit_months}-month exit`;
      case 'RENTAL': return `Best long-term hold: $${strategy.monthly_cash_flow}/mo cash flow, ${strategy.cash_on_cash_return_pct}% CoC return`;
      case 'TERMS': return `Only viable path: bridges $${strategy.gap_bridged.toLocaleString()} gap with seller financing`;
      default: return 'Best overall score';
    }
  }

  getStrategyOutcome(strategyName, strategy, winner) {
    const won = strategy.strategy === winner.strategy;
    
    if (strategyName === 'flip') {
      if (won) return `Won: ${strategy.roi_pct}% ROI beats alternatives`;
      if (strategy.roi_pct < 15) return `Lost: ROI too low (${strategy.roi_pct}%)`;
      return `Lost: Lower score than ${winner.strategy}`;
    }

    if (strategyName === 'rental') {
      if (!strategy.viable) return 'Lost: Negative cash flow';
      if (won) return `Won: Strong cash flow ($${strategy.monthly_cash_flow}/mo)`;
      if (strategy.rent_confidence < 60) return 'Lost: Rent estimate has low confidence';
      return `Lost: Lower score than ${winner.strategy}`;
    }

    if (strategyName === 'terms') {
      if (!strategy.viable) return strategy.reason || 'Not viable';
      if (won) return `Won: Only way to bridge gap ($${strategy.gap_bridged.toLocaleString()})`;
      return `Lost: Cash strategies more attractive`;
    }

    return 'Analyzed';
  }

  getDecisionDriver(flip, rental, terms, winner) {
    if (winner.strategy === 'FLIP' && parseFloat(flip.roi_pct) >= 25) return 'Strong flip profit margin';
    if (winner.strategy === 'RENTAL' && parseFloat(rental.cash_on_cash_return_pct) >= 10) return 'High cash-on-cash return for rental';
    if (winner.strategy === 'TERMS') return 'Price gap requires creative structure';
    if (flip.score > rental.score + 20) return 'Flip ROI significantly outperforms';
    if (rental.score > flip.score + 20) return 'Rental cash flow and appreciation upside';
    return 'Marginal difference - choose based on goals';
  }
}

// =========================================================================
// 5. DEAL DECISION ENGINE - Top-line decisions and lead staging
// =========================================================================

class DealDecisionEngine {
  constructor(arvResult, maoResult, sellerAsk, strategyResult = null) {
    this.arv = arvResult;
    this.mao = maoResult;
    this.sellerAsk = sellerAsk;
    this.strategy = strategyResult;
  }

  getManualCompRequired() {
    const reasons = [];

    if (this.arv.arv_comp_count < 2) reasons.push('Fewer than 2 ARV comps');
    if (this.arv.confidence_score < 60) reasons.push('Low confidence score');
    if (this.arv.comp_quality_score < 50) reasons.push('Poor comp quality');

    if (this.arv.comp_details && this.arv.comp_details.arv_comps.length >= 2) {
      const prices = this.arv.comp_details.arv_comps.map(c => c.adjusted_price);
      const avg = prices.reduce((sum, p) => sum + p, 0) / prices.length;
      const variance = prices.reduce((sum, p) => sum + Math.pow(p - avg, 2), 0) / prices.length;
      const stdDev = Math.sqrt(variance);
      const coefficientOfVariation = stdDev / avg;
      if (coefficientOfVariation > 0.20) reasons.push('Wide comp price spread (>20%)');
    }

    if (this.arv.manual_override_flag && this.arv.override_variance_pct > 10) {
      reasons.push('Manual override exceeds comp support by >10%');
    }

    if (this.arv.status === 'INSUFFICIENT_ARV_COMPS' || this.arv.status === 'WARNING') {
      reasons.push('Insufficient ARV comp data');
    }

    return { required: reasons.length > 0, reasons };
  }

  getLeadStage() {
    const manualCompCheck = this.getManualCompRequired();
    
    if (!this.sellerAsk || this.arv.arv_comp_count === 0) {
      return {
        stage: 'SCREENING',
        description: 'Initial data gathering',
        next_action: 'Collect property details and seller motivation'
      };
    }

    if (manualCompCheck.required || this.arv.confidence_score < 70) {
      return {
        stage: 'REVIEW',
        description: 'Manual comp work required',
        next_action: 'Pull and analyze additional comps, verify property condition'
      };
    }

    if (this.arv.confidence_score >= 70 && this.arv.comp_quality_score >= 60) {
      const gap = this.sellerAsk - this.mao.target_mao;
      const gapPct = (gap / this.sellerAsk) * 100;
      
      if (gapPct > 25) {
        return {
          stage: 'UNDERWRITE',
          description: 'Analyzing negotiation strategy',
          next_action: 'Determine if terms or creative structure can bridge gap'
        };
      }
    }

    if (this.arv.confidence_score >= 80 && this.arv.comp_quality_score >= 70 &&
        this.sellerAsk <= this.mao.max_mao * 1.25) {
      return {
        stage: 'OFFER_READY',
        description: 'Ready for offer presentation',
        next_action: 'Prepare offer package and negotiate'
      };
    }

    return {
      stage: 'REVIEW',
      description: 'Additional analysis needed',
      next_action: 'Verify assumptions and improve data quality'
    };
  }

  getDealDecision() {
    if (!this.sellerAsk) {
      return {
        decision: 'NEEDS_REVIEW',
        reason: 'Seller ask not yet obtained',
        confidence: 'LOW',
        recommended_action: 'Obtain seller price expectations',
        urgency: 'MEDIUM'
      };
    }

    const gap = this.sellerAsk - this.mao.max_mao;
    const gapPct = (gap / this.sellerAsk) * 100;
    const manualCompCheck = this.getManualCompRequired();

    if (this.arv.confidence_score >= 70 && !manualCompCheck.required && this.sellerAsk <= this.mao.max_mao) {
      return {
        decision: 'PURSUE',
        reason: 'Seller ask within range, good comp support',
        confidence: 'HIGH',
        recommended_action: 'Make offer at target MAO',
        urgency: 'HIGH'
      };
    }

    if (this.arv.confidence_score >= 70 && !manualCompCheck.required && 
        this.sellerAsk > this.mao.max_mao && gapPct <= 15) {
      return {
        decision: 'PURSUE',
        reason: 'Close to range, terms path likely viable',
        confidence: 'MEDIUM',
        recommended_action: 'Explore seller financing or creative structure',
        urgency: 'MEDIUM'
      };
    }

    if (gapPct > 40 && this.arv.confidence_score >= 60) {
      return {
        decision: 'PASS',
        reason: `Seller ask is ${gapPct.toFixed(0)}% above max acceptable offer`,
        confidence: 'HIGH',
        recommended_action: 'Pass unless seller shows extreme motivation',
        urgency: 'LOW'
      };
    }

    if (manualCompCheck.required && this.arv.confidence_score < 50 && this.sellerAsk > this.mao.target_mao) {
      return {
        decision: 'PASS',
        reason: 'Insufficient comp data and seller expectations too high',
        confidence: 'MEDIUM',
        recommended_action: 'Move to next lead',
        urgency: 'LOW'
      };
    }

    if (this.arv.confidence_score >= 50 && this.arv.confidence_score < 70) {
      return {
        decision: 'NEEDS_REVIEW',
        reason: 'Moderate confidence - manual comp work recommended',
        confidence: 'MEDIUM',
        recommended_action: 'Pull additional comps, verify condition',
        urgency: 'MEDIUM'
      };
    }

    if (gapPct >= 15 && gapPct <= 40 && this.arv.confidence_score >= 60) {
      return {
        decision: 'NEEDS_REVIEW',
        reason: 'Deal close enough to analyze creative structure',
        confidence: 'MEDIUM',
        recommended_action: 'Analyze terms and seller motivation',
        urgency: 'MEDIUM'
      };
    }

    if (manualCompCheck.required) {
      return {
        decision: 'NEEDS_REVIEW',
        reason: 'Comp quality issues require manual review',
        confidence: 'LOW',
        recommended_action: 'Complete manual comp analysis',
        urgency: 'MEDIUM'
      };
    }

    return {
      decision: 'NEEDS_REVIEW',
      reason: 'Additional analysis needed to determine viability',
      confidence: 'MEDIUM',
      recommended_action: 'Gather more data and reassess',
      urgency: 'MEDIUM'
    };
  }

  getDecisionPackage() {
    const manualCompCheck = this.getManualCompRequired();
    const leadStage = this.getLeadStage();
    const dealDecision = this.getDealDecision();

    return {
      decision: dealDecision.decision,
      decision_reason: dealDecision.reason,
      decision_confidence: dealDecision.confidence,
      recommended_action: dealDecision.recommended_action,
      urgency: dealDecision.urgency,
      lead_stage: leadStage.stage,
      stage_description: leadStage.description,
      next_action: leadStage.next_action,
      manual_comp_required: manualCompCheck.required,
      manual_comp_reasons: manualCompCheck.reasons,
      seller_ask: this.sellerAsk,
      gap_to_max_mao: this.sellerAsk - this.mao.max_mao,
      gap_percentage: ((this.sellerAsk - this.mao.max_mao) / this.sellerAsk) * 100,
      arv_confidence: this.arv.confidence_score,
      comp_quality: this.arv.comp_quality_score,
      arv_comp_count: this.arv.arv_comp_count
    };
  }
}

// =========================================================================
// 6. MAIN UNIFIED DEAL ANALYZER CLASS
// =========================================================================

class DealAnalyzer {
  constructor(subjectProperty, comps, dealParams) {
    this.property = subjectProperty;
    this.comps = comps;
    this.dealParams = dealParams;
  }

  /**
   * Main analysis function - returns complete deal package
   * 
   * @param {number} sellerAsk - Seller's asking price
   * @param {object} rentData - { monthlyRent, rentComps, isManualOverride }
   * @param {number} manualARV - Optional manual ARV override
   * @param {boolean} useManualOverride - Whether to use manual override in MAO calcs
   * @returns {object} Complete deal analysis
   */
  analyze(sellerAsk = null, rentData = {}, manualARV = null, useManualOverride = false) {
    // Step 1: Calculate ARV
    const arvCalc = new ARVCalculator(this.property, this.comps);
    const arvResult = arvCalc.calculate(manualARV);

    // Step 2: Calculate MAO
    const maoCalc = new MAOCalculator({
      ...this.dealParams,
      useManualOverride: useManualOverride
    });
    const maoResult = maoCalc.calculate(arvResult);

    // Step 3: Analyze strategies
    const strategyCalc = new StrategyAnalyzer(
      this.property, 
      arvResult, 
      maoResult,
      {
        rentEstimate: rentData.monthlyRent,
        rentComps: rentData.rentComps || []
      }
    );
    const strategyResult = strategyCalc.compareStrategies(sellerAsk, rentData);

    // Step 4: Get deal decision
    const decisionEngine = new DealDecisionEngine(
      arvResult, 
      maoResult, 
      sellerAsk, 
      strategyResult
    );
    const decisionPackage = decisionEngine.getDecisionPackage();

    // Return unified result
    return {
      // Property info
      property: this.property,
      
      // ARV & Valuation
      ...arvResult,
      
      // MAO Structure
      ...maoResult,
      
      // Strategy Comparison
      ...strategyResult,
      
      // Deal Decision & Staging
      ...decisionPackage
    };
  }

  /**
   * Quick ballpark check - are we even close?
   */
  quickCheck(sellerAsk) {
    const arvCalc = new ARVCalculator(this.property, this.comps);
    const arvResult = arvCalc.calculate();
    const maoCalc = new MAOCalculator(this.dealParams);
    const maoResult = maoCalc.calculate(arvResult);

    const gap = sellerAsk - maoResult.max_mao;
    const gapPct = (gap / sellerAsk) * 100;

    let status;
    if (sellerAsk <= maoResult.max_mao) status = 'IN_RANGE';
    else if (gapPct < 20) status = 'NEGOTIABLE';
    else if (gapPct < 40) status = 'TOUGH_NEGOTIATION';
    else status = 'NOT_IN_BALLPARK';

    return {
      seller_ask: sellerAsk,
      max_mao: maoResult.max_mao,
      gap: gap,
      gap_pct: gapPct,
      status: status,
      confidence: arvResult.confidence_score
    };
  }
}

// =========================================================================
// EXPORT
// =========================================================================

module.exports = DealAnalyzer;

// Also export individual classes for advanced usage
module.exports.CompAnalyzer = CompAnalyzer;
module.exports.ARVCalculator = ARVCalculator;
module.exports.MAOCalculator = MAOCalculator;
module.exports.StrategyAnalyzer = StrategyAnalyzer;
module.exports.DealDecisionEngine = DealDecisionEngine;
