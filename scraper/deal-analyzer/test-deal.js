/**
 * TEST DEAL ANALYZER
 * Simple test to verify the analyzer works
 */

const DealAnalyzer = require('./dealAnalyzer.unified.js');

console.log('Starting Deal Analyzer Test...\n');

// Example Akron property
const property = {
  address: '123 Summit Lake Ave, Akron, OH',
  property_type: 'Single Family',
  sqft: 1200,
  baths_full: 2,
  garage_spaces: 0,
  lot_sqft: 5000,
  subdivision: 'Summit Lake'
};

// Example comps from Akron area
const comps = [
  {
    address: '125 Summit Lake Ave',
    property_type: 'Single Family',
    sqft: 1150,
    baths_full: 2,
    garage_spaces: 0,
    sale_price: 45000,
    sale_date: '2025-12-15',
    status: 'sold',
    subdivision: 'Summit Lake',
    remarks: 'recently renovated, new kitchen and bath'
  },
  {
    address: '130 Summit Lake Ave',
    property_type: 'Single Family',
    sqft: 1280,
    baths_full: 2,
    garage_spaces: 1,
    sale_price: 52000,
    sale_date: '2026-01-20',
    status: 'sold',
    subdivision: 'Summit Lake',
    remarks: 'fully updated, move-in ready'
  },
  {
    address: '140 Summit Lake Ave',
    property_type: 'Single Family',
    sqft: 1180,
    baths_full: 2,
    garage_spaces: 0,
    sale_price: 48000,
    sale_date: '2025-11-10',
    status: 'sold',
    subdivision: 'Summit Lake',
    remarks: 'updated kitchen, new flooring'
  }
];

// Deal parameters (adjust these for your market)
const dealParams = {
  rehabCost: 20000,        // Estimated rehab cost
  holdingCosts: 2500,      // Holding costs during project
  closingCosts: 1500,      // Closing costs
  financingCosts: 1000,    // Financing costs
  lowballProfit: 0.30,     // 30% profit for lowball offer
  targetProfit: 0.20,      // 20% profit for target offer
  maxProfit: 0.12,         // 12% minimum profit for walk-away
  buyerCosts: 0.03,        // 3% buyer costs
  sellerCosts: 0.06        // 6% seller costs when you sell
};

// Create the analyzer
const analyzer = new DealAnalyzer(property, comps, dealParams);

// Seller asking price
const sellerAsk = 35000;

console.log('═'.repeat(70));
console.log('DEAL ANALYSIS RESULTS');
console.log('═'.repeat(70));
console.log(`Property: ${property.address}`);
console.log(`Size: ${property.sqft} sqft, ${property.baths_full} baths`);
console.log(`Seller Asking: $${sellerAsk.toLocaleString()}`);
console.log('');

// Run the analysis
const result = analyzer.analyze(sellerAsk);

// Display Decision
console.log('┌─ DECISION ─────────────────────────────────────────────────────┐');
console.log(`│ ${result.decision.padEnd(62)} │`);
console.log(`│ ${result.decision_reason.padEnd(62)} │`);
console.log(`│ Lead Stage: ${result.lead_stage.padEnd(49)} │`);
console.log('└────────────────────────────────────────────────────────────────┘');
console.log('');

// Display Offer Structure
console.log('┌─ OFFER STRUCTURE ──────────────────────────────────────────────┐');
console.log(`│ Opener (Lowball):  $${result.lowball_mao.toLocaleString().padStart(10)}  (${result.parameters.lowball_profit_pct}% profit)     │`);
console.log(`│ Target:            $${result.target_mao.toLocaleString().padStart(10)}  (${result.parameters.target_profit_pct}% profit)     │`);
console.log(`│ Walk-Away (Max):   $${result.max_mao.toLocaleString().padStart(10)}  (${result.parameters.max_profit_pct}% profit)      │`);
console.log(`│ Gap to Max:        $${result.gap_to_max_mao.toLocaleString().padStart(10)}  (${result.gap_percentage.toFixed(0)}%)                │`);
console.log('└────────────────────────────────────────────────────────────────┘');
console.log('');

// Display Valuation
console.log('┌─ VALUATION ────────────────────────────────────────────────────┐');
console.log(`│ As-Is Value:           $${(result.as_is_value || 0).toLocaleString().padStart(10)}                    │`);
console.log(`│ Comp-Supported ARV:    $${(result.comp_supported_arv || 0).toLocaleString().padStart(10)}                    │`);
console.log(`│ Confidence Score:      ${result.confidence_score}/100                              │`);
console.log(`│ Comp Quality Score:    ${result.comp_quality_score}/100                              │`);
console.log(`│ ARV Comps Used:        ${result.arv_comp_count}                                     │`);
console.log('└────────────────────────────────────────────────────────────────┘');
console.log('');

// Display Strategy
console.log('┌─ RECOMMENDED STRATEGY ─────────────────────────────────────────┐');
console.log(`│ Strategy: ${result.recommended_strategy.padEnd(54)} │`);
console.log(`│ Score: ${result.recommended_score}/100                                            │`);
console.log(`│ Reason: ${result.comparison.winner_reason.substring(0, 53).padEnd(53)} │`);
console.log('└────────────────────────────────────────────────────────────────┘');
console.log('');

// Display Strategy Details
if (result.strategies.flip) {
  console.log('FLIP Analysis:');
  console.log(`  ROI: ${result.strategies.flip.roi_pct}%`);
  console.log(`  Net Profit: $${result.strategies.flip.net_profit.toLocaleString()}`);
  console.log(`  Outcome: ${result.comparison.flip_outcome}`);
  console.log('');
}

if (result.strategies.rental && result.strategies.rental.viable) {
  console.log('RENTAL Analysis:');
  console.log(`  Monthly Cash Flow: $${result.strategies.rental.monthly_cash_flow}`);
  console.log(`  Cash-on-Cash Return: ${result.strategies.rental.cash_on_cash_return_pct}%`);
  console.log(`  Rent Confidence: ${result.strategies.rental.rent_confidence}/100`);
  console.log(`  Outcome: ${result.comparison.rental_outcome}`);
  console.log('');
}

// Manual Review Required?
if (result.manual_comp_required) {
  console.log('⚠️  MANUAL REVIEW REQUIRED:');
  result.manual_comp_reasons.forEach(reason => {
    console.log(`   • ${reason}`);
  });
  console.log('');
}

// Next Action
console.log('┌─ NEXT ACTION ──────────────────────────────────────────────────┐');
console.log(`│ ${result.next_action.padEnd(62)} │`);
console.log('└────────────────────────────────────────────────────────────────┘');
console.log('');

console.log('═'.repeat(70));
console.log('Test Complete!');
console.log('═'.repeat(70));
