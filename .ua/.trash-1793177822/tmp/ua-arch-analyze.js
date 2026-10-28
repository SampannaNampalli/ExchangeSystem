const fs=require('fs');
try{
const inp=JSON.parse(fs.readFileSync(process.argv[2],'utf8'));
const nodes=inp.fileNodes, imp=inp.importEdges||[], all=inp.allEdges||[];
const ids=new Set(nodes.map(n=>n.id)); const byId={}; nodes.forEach(n=>byId[n.id]=n);
// group: strip src/main/java/exchangeserver or src/test/java/exchangeserver
function group(p){
  let m=p.match(/^src\/(main|test)\/java\/exchangeserver\/(.*)$/);
  if(m){const parts=m[2].split('/');const g=parts.length>1?parts[0]:'(root-pkg)';return (m[1]==='test'?'test/':'')+g;}
  const parts=p.split('/'); if(parts.length===1) return '(root)';
  if(parts[0]==='docs') return parts.length>2? 'docs/'+parts[1]:'docs';
  return parts[0];
}
const dg={},ng={},gOf={};
nodes.forEach(n=>{const g=group(n.filePath);gOf[n.id]=g;(dg[g]=dg[g]||[]).push(n.id);(ng[n.type]=ng[n.type]||[]).push(n.id);});
const fanIn={},fanOut={},inter={},intra={};
Object.keys(dg).forEach(g=>intra[g]={internalEdges:0,totalEdges:0});
imp.forEach(e=>{if(!ids.has(e.source)||!ids.has(e.target))return;
 fanOut[e.source]=(fanOut[e.source]||0)+1;fanIn[e.target]=(fanIn[e.target]||0)+1;
 const a=gOf[e.source],b=gOf[e.target];
 if(a===b){intra[a].internalEdges++;intra[a].totalEdges++;}else{intra[a].totalEdges++;intra[b].totalEdges++;const k=a+'->'+b;inter[k]=(inter[k]||0)+1;}});
Object.values(intra).forEach(v=>v.density=v.totalEdges?+(v.internalEdges/v.totalEdges).toFixed(2):0);
const cc={};all.forEach(e=>{const s=byId[e.source],t=byId[e.target];if(!s||!t)return;if(s.type===t.type&&e.type==='imports')return;const k=s.type+'|'+t.type+'|'+e.type;cc[k]=(cc[k]||0)+1;});
const pat={domain:'service',book:'data',engine:'service',command:'service',journal:'data',core:'service',gateway:'api',client:'entry',bench:'test',testing:'test',docs:'documentation','.mvn':'config'};
const pm={};Object.keys(dg).forEach(g=>{if(g.startsWith('test/'))pm[g]='test';else{const b=g.split('/').pop();pm[g]=pat[b]||pat[g.split('/')[0]]||null;}});
const interArr=Object.entries(inter).map(([k,c])=>{const [f,t]=k.split('->');return{from:f,to:t,count:c}}).sort((x,y)=>y.count-x.count);
const dir=[];const seen=new Set();interArr.forEach(({from,to,count})=>{const k=[from,to].sort().join('|');if(seen.has(k))return;seen.add(k);const rev=inter[to+'->'+from]||0;dir.push(count>=rev?{dependent:from,dependsOn:to,forward:count,reverse:rev}:{dependent:to,dependsOn:from,forward:rev,reverse:count});});
const infra=nodes.filter(n=>/(^|\/)(Dockerfile|docker-compose|Makefile|mvnw)|\.github\/|Jenkinsfile/.test(n.filePath)).map(n=>n.filePath);
const docGroups=new Set(nodes.filter(n=>n.type==='document').map(n=>gOf[n.id]));
const groups=Object.keys(dg);
const out={scriptCompleted:true,directoryGroups:dg,nodeTypeGroups:ng,
 crossCategoryEdges:Object.entries(cc).map(([k,c])=>{const [f,t,e]=k.split('|');return{fromType:f,toType:t,edgeType:e,count:c}}),
 interGroupImports:interArr,intraGroupDensity:intra,patternMatches:pm,
 deploymentTopology:{hasDockerfile:false,hasCompose:false,hasK8s:false,hasTerraform:false,hasCI:false,infraFiles:infra},
 dataPipeline:{schemaFiles:[],migrationFiles:[],dataModelFiles:dg['book']||[],apiHandlerFiles:dg['gateway']||[]},
 docCoverage:{groupsWithDocs:docGroups.size,totalGroups:groups.length,coverageRatio:+(docGroups.size/groups.length).toFixed(2),undocumentedGroups:groups.filter(g=>!docGroups.has(g))},
 dependencyDirection:dir,
 fileStats:{totalFileNodes:nodes.length,filesPerGroup:Object.fromEntries(groups.map(g=>[g,dg[g].length])),nodeTypeCounts:Object.fromEntries(Object.entries(ng).map(([k,v])=>[k,v.length]))},
 fileFanIn:fanIn,fileFanOut:fanOut};
fs.writeFileSync(process.argv[3],JSON.stringify(out,null,2));process.exit(0);
}catch(e){console.error(e.stack);process.exit(1);}
