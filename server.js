const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const { MongoClient } = require('mongodb');
const { Telegraf } = require('telegraf');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const cookieParser = require('cookie-parser');
const axios = require('axios');
const cors = require('cors');

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: {
    origin: true,
    credentials: true
  }
});

app.use(cors({
  origin: true,
  credentials: true
}));
app.use(express.json());
app.use(cookieParser());

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017';
const DB_NAME = process.env.DB_NAME || 'game_db';

if (!process.env.JWT_SECRET) {
  throw new Error('JWT_SECRET environment variable must be set');
}
const JWT_SECRET = process.env.JWT_SECRET;

const BOT_TOKEN = process.env.BOT_TOKEN || 'YOUR_BOT_TOKEN';
const PORT = process.env.PORT || 3000;

let db;
let usersCollection;
let roomsCollection;
let gamesCollection;
let matchmakingCollection;
let challengesCollection;
const bot = new Telegraf(BOT_TOKEN);

// Game state storage
const activeGames = new Map();

// LRU Cache for verified words (prevents memory buildup)
class LRUCache {
  constructor(maxSize = 5000) {
    this.maxSize = maxSize;
    this.cache = new Map();
  }
  
  get(key) {
    if (!this.cache.has(key)) return undefined;
    // Move to end (most recently used)
    const value = this.cache.get(key);
    this.cache.delete(key);
    this.cache.set(key, value);
    return value;
  }
  
  set(key, value) {
    // Remove if exists (to re-add at end)
    if (this.cache.has(key)) {
      this.cache.delete(key);
    }
    // Remove oldest if at capacity
    else if (this.cache.size >= this.maxSize) {
      const firstKey = this.cache.keys().next().value;
      this.cache.delete(firstKey);
    }
    this.cache.set(key, value);
  }
  
  has(key) {
    return this.cache.has(key);
  }
  
  get size() {
    return this.cache.size;
  }
}

const verifiedWordsCache = new LRUCache(5000);

// Central timer registry to prevent memory leaks
const gameTimers = new Map(); // roomId -> timer
const disconnectTimers = new Map(); // userId -> timer

// Hard limit on active games
const MAX_ACTIVE_GAMES = 1000;

// Challenge definitions
const DAILY_CHALLENGES = [
  { id: 'win_2_ranked', name: 'Ranked Victor', description: 'Win 2 ranked games', requirement: 2, reward: { coins: 100, points: 50 } },
  { id: 'play_7_letter', name: 'Word Master', description: 'Play a 7+ letter word', requirement: 1, reward: { coins: 75, points: 30 } },
  { id: 'win_no_timeout', name: 'Perfect Game', description: 'Win without timing out once', requirement: 1, reward: { coins: 150, points: 75 } },
  { id: 'play_5_games', name: 'Dedicated Player', description: 'Play 5 games', requirement: 5, reward: { coins: 50, points: 25 } }
];

const WEEKLY_CHALLENGES = [
  { id: 'win_10_ranked', name: 'Weekly Warrior', description: 'Win 10 ranked games', requirement: 10, reward: { coins: 500, points: 250 } },
  { id: 'reach_gold', name: 'Golden Achievement', description: 'Reach Gold rank or higher', requirement: 1, reward: { coins: 300, points: 150 } },
  { id: 'play_30_games', name: 'Grinder', description: 'Play 30 games', requirement: 30, reward: { coins: 200, points: 100 } },
  { id: 'longest_word_9', name: 'Vocabulary Expert', description: 'Play a 9+ letter word', requirement: 1, reward: { coins: 250, points: 125 } }
];

// Matchmaking queues by mode
const matchmakingQueues = {
  'normalclassic_1v1': [],
  'normalclassic_4player': [],
  'hardclassic_1v1': [],
  'hardclassic_4player': [],
  'normalrnd_1v1': [],
  'normalrnd_4player': [],
  'hardrnd_1v1': [],
  'hardrnd_4player': []
};

// Rank system configuration
const RANK_SYSTEM = {
  bronze: { winsRequired: 3, minWins: 0 },
  silver: { winsRequired: 5, minWins: 3 },
  gold: { winsRequired: 6, minWins: 8 },
  diamond: { winsRequired: 8, minWins: 14 },
  legend: { winsRequired: 15, minWins: 22 },
  mythic: { winsRequired: Infinity, minWins: 37 } // Infinite progression
};

const RANK_ORDER = ['bronze', 'silver', 'gold', 'diamond', 'legend', 'mythic'];

// MongoDB connection
MongoClient.connect(MONGO_URI)
  .then(client => {
    db = client.db(DB_NAME);
    usersCollection = db.collection('users');
    roomsCollection = db.collection('rooms');
    gamesCollection = db.collection('games');
    matchmakingCollection = db.collection('matchmaking');
    challengesCollection = db.collection('challenges');
    
    // Create indexes
    usersCollection.createIndex({ userId: 1 }, { unique: true });
    usersCollection.createIndex({ telegramId: 1 }, { unique: true });
    usersCollection.createIndex({ playerName: 1 });
    usersCollection.createIndex({ playerTag: 1 });
    usersCollection.createIndex({ 'rankedStats.normalclassic.mythicRank': -1 });
    usersCollection.createIndex({ 'rankedStats.hardclassic.mythicRank': -1 });
    usersCollection.createIndex({ 'rankedStats.normalrnd.mythicRank': -1 });
    usersCollection.createIndex({ 'rankedStats.hardrnd.mythicRank': -1 });
    
    roomsCollection.createIndex({ roomId: 1 }, { unique: true });
    roomsCollection.createIndex({ creatorId: 1 });
    roomsCollection.createIndex({ status: 1 });
    roomsCollection.createIndex(
      { createdAt: 1 }, 
      { 
        expireAfterSeconds: 600,
        partialFilterExpression: { status: 'waiting' } // Only delete waiting rooms after 10 mins
      }
    );
    
    gamesCollection.createIndex({ roomId: 1 }, { unique: true });
    gamesCollection.createIndex({ status: 1 });
    gamesCollection.createIndex({ createdAt: 1 });
    
    matchmakingCollection.createIndex({ userId: 1 });
    matchmakingCollection.createIndex({ mode: 1 });
    matchmakingCollection.createIndex({ createdAt: 1 }, { expireAfterSeconds: 180 }); // Auto-cleanup after 3 mins
    
    // Challenges collection for daily/weekly tasks
    const challengesCollection = db.collection('challenges');
    challengesCollection.createIndex({ userId: 1 });
    challengesCollection.createIndex({ type: 1 }); // 'daily' or 'weekly'
    challengesCollection.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 });
    
    console.log('MongoDB connected and indexes created');
  })
  .catch(err => {
    console.error('MongoDB connection error:', err);
    process.exit(1);
  });

// Generate unique user ID using crypto UUID (collision-resistant)
function generateUserId() {
  // Generate numeric ID from UUID to maintain compatibility
  const uuid = crypto.randomUUID().replace(/-/g, '');
  const numericId = parseInt(uuid.substring(0, 13), 16); // 13 hex digits = ~52 bits
  return numericId;
}

// Generate 6-digit room ID
function generateRoomId() {
  return Math.floor(100000 + Math.random() * 900000);
}

// Get random letter
function getRandomLetter() {
  const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  return letters[Math.floor(Math.random() * letters.length)];
}

// Get time limit based on minimum letters
function getTimeLimit(minLetters) {
  // 40 seconds for 3 letters, decreasing to 20 seconds for 10+ letters
  const timeMap = {
    3: 40,
    4: 37,
    5: 34,
    6: 31,
    7: 28,
    8: 25,
    9: 22,
    10: 20
  };
  return timeMap[minLetters] || 20;
}

// Verify word using API
async function verifyWord(word) {
  const wordLower = word.toLowerCase();
  
  // Check cache first
  if (verifiedWordsCache.has(wordLower)) {
    return verifiedWordsCache.get(wordLower);
  }
  
  try {
    const response = await axios.get(`https://saulwords-api.vercel.app/check?word=${wordLower}`, {
      timeout: 3000 // 3 second timeout
    });
    
    const isValid = response.data.valid === true;
    
    // Cache the result (LRU will handle eviction)
    verifiedWordsCache.set(wordLower, isValid);
    
    return isValid;
  } catch (error) {
    console.error('Word verification error:', error.message);
    
    // If API is down: ONLY accept words already in cache, reject uncached words
    // This prevents cheating while not hard-locking the game
    if (verifiedWordsCache.has(wordLower)) {
      console.warn(`API failure for word "${word}" - using cached result`);
      return verifiedWordsCache.get(wordLower);
    }
    
    console.warn(`API failure for word "${word}" - rejecting uncached word to prevent abuse`);
    return false;
  }
}

// Calculate points based on game stats
function calculatePoints(playerStats, isWinner, totalPlayers) {
  let points = 0;
  
  // Winner bonus
  if (isWinner) {
    points += 100;
  }
  
  // Placement points (based on elimination order)
  const placement = playerStats.placement || totalPlayers;
  points += (totalPlayers - placement + 1) * 10;
  
  // Longest word bonus (1 point per letter)
  points += (playerStats.longestWord?.length || 0) * 2;
  
  // Valid words bonus
  points += (playerStats.wordsUsed?.length || 0) * 5;
  
  return points;
}

// Calculate level from points (100 points per level)
function calculateLevel(points) {
  return Math.floor(points / 100) + 1;
}

// Get rank and position from wins
function getRankFromWins(mode, totalWins) {
  if (totalWins < RANK_SYSTEM.silver.minWins) {
    return { rank: 'bronze', position: totalWins };
  } else if (totalWins < RANK_SYSTEM.gold.minWins) {
    return { rank: 'silver', position: totalWins - RANK_SYSTEM.silver.minWins };
  } else if (totalWins < RANK_SYSTEM.diamond.minWins) {
    return { rank: 'gold', position: totalWins - RANK_SYSTEM.gold.minWins };
  } else if (totalWins < RANK_SYSTEM.legend.minWins) {
    return { rank: 'diamond', position: totalWins - RANK_SYSTEM.diamond.minWins };
  } else if (totalWins < RANK_SYSTEM.mythic.minWins) {
    return { rank: 'legend', position: totalWins - RANK_SYSTEM.legend.minWins };
  } else {
    return { rank: 'mythic', position: 0, mythicRank: totalWins - RANK_SYSTEM.mythic.minWins };
  }
}

// Update user rank after game (fully atomic with proper deranking)
async function updateUserRank(userId, mode, isWinner) {
  const modeKey = mode.replace('_1v1', '').replace('_4player', '');
  
  // Calculate win/loss deltas based on outcome
  const session = db.client ? db.client.startSession() : null;
  
  try {
    if (session) session.startTransaction();
    
    // Get current user state
    const user = await usersCollection.findOne({ userId }, { session });
    const currentStats = user.rankedStats[modeKey];
    const currentRank = currentStats.rank;
    const currentPosition = currentStats.position;
    const currentMythicRank = currentStats.mythicRank || 0;
    const currentSeason = user.currentSeason || 1;
    
    let newWins = currentStats.wins;
    let newLosses = currentStats.losses;
    let newRank, newPosition, newMythicRank = 0;
    
    if (isWinner) {
      // WIN: Increment wins and advance rank
      newWins++;
      newLosses = currentStats.losses; // Don't change losses on win
      
      const rankData = getRankFromWins(modeKey, newWins);
      newRank = rankData.rank;
      newPosition = rankData.position;
      newMythicRank = rankData.mythicRank || 0;
      
    } else {
      // LOSS: Derank logic
      newLosses++;
      
      if (currentRank === 'mythic') {
        // Mythic can derank
        if (currentMythicRank > 0) {
          // Mythic X -> Mythic X-1
          newWins--;
          newRank = 'mythic';
          newMythicRank = currentMythicRank - 1;
          newPosition = 0;
        } else {
          // Mythic 0 -> Legend (highest position)
          newWins = RANK_SYSTEM.mythic.minWins - 1; // Back to legend
          newRank = 'legend';
          newPosition = RANK_SYSTEM.legend.winsRequired - 1; // Highest position in legend
          newMythicRank = 0;
        }
      } else if (currentPosition > 0) {
        // Has position to lose in current rank
        newWins--;
        const rankData = getRankFromWins(modeKey, newWins);
        newRank = rankData.rank;
        newPosition = rankData.position;
        newMythicRank = 0;
      } else if (currentRank !== 'bronze') {
        // At position 0 of current rank (not bronze) -> derank to highest position of previous rank
        const rankIndex = RANK_ORDER.indexOf(currentRank);
        const previousRankName = RANK_ORDER[rankIndex - 1];
        
        if (previousRankName) {
          newRank = previousRankName;
          newPosition = RANK_SYSTEM[previousRankName].winsRequired - 1; // Highest position in previous rank
          newWins = RANK_SYSTEM[previousRankName].minWins + newPosition;
          newMythicRank = 0;
        }
      } else {
        // Bronze 0 - can't derank
        newRank = 'bronze';
        newPosition = 0;
        newWins = 0;
        newMythicRank = 0;
      }
    }
    
    // Check if this is a new highest rank
    const updateFields = {
      [`rankedStats.${modeKey}.rank`]: newRank,
      [`rankedStats.${modeKey}.position`]: newPosition,
      [`rankedStats.${modeKey}.mythicRank`]: newMythicRank,
      [`rankedStats.${modeKey}.wins`]: newWins,
      [`rankedStats.${modeKey}.losses`]: newLosses
    };
    
    // Update highest rank if this is better
    const currentHighest = user.highestRanks?.[modeKey] || { rank: 'bronze', mythicRank: 0, season: 0 };
    const newRankIndex = RANK_ORDER.indexOf(newRank);
    const highestRankIndex = RANK_ORDER.indexOf(currentHighest.rank);
    
    if (newRankIndex > highestRankIndex || 
        (newRankIndex === highestRankIndex && newRank === 'mythic' && newMythicRank > currentHighest.mythicRank)) {
      updateFields[`highestRanks.${modeKey}.rank`] = newRank;
      updateFields[`highestRanks.${modeKey}.mythicRank`] = newMythicRank;
      updateFields[`highestRanks.${modeKey}.season`] = currentSeason;
    }
    
    // Single atomic update
    await usersCollection.updateOne(
      { userId },
      { $set: updateFields },
      { session }
    );
    
    if (session) await session.commitTransaction();
    
    // Calculate rank change for display
    const oldRankIndex = RANK_ORDER.indexOf(currentRank);
    const newRankIndex = RANK_ORDER.indexOf(newRank);
    
    let rankChange = 'same';
    if (newRankIndex > oldRankIndex) {
      rankChange = 'promoted';
    } else if (newRankIndex < oldRankIndex) {
      rankChange = 'demoted';
    } else if (newRank === 'mythic' && newMythicRank !== currentMythicRank) {
      rankChange = newMythicRank > currentMythicRank ? 'up' : 'down';
    } else if (newPosition !== currentPosition) {
      rankChange = newPosition > currentPosition ? 'up' : 'down';
    }
    
    return { 
      rank: newRank, 
      position: newPosition, 
      mythicRank: newMythicRank,
      rankChange,
      oldRank: currentRank,
      oldPosition: currentPosition,
      oldMythicRank: currentMythicRank
    };
    
  } catch (error) {
    if (session) await session.abortTransaction();
    throw error;
  } finally {
    if (session) session.endSession();
  }
}

// Validate Telegram initData
function validateTelegramInit(initData) {
  try {
    const urlParams = new URLSearchParams(initData);
    const hash = urlParams.get('hash');
    urlParams.delete('hash');
    
    const dataCheckString = Array.from(urlParams.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, value]) => `${key}=${value}`)
      .join('\n');
    
    const secretKey = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
    const calculatedHash = crypto.createHmac('sha256', secretKey).update(dataCheckString).digest('hex');
    
    if (calculatedHash !== hash) {
      return null;
    }
    
    const user = urlParams.get('user');
    if (user) {
      return JSON.parse(user);
    }
    
    return null;
  } catch (error) {
    return null;
  }
}

// JWT middleware
function authenticateToken(req, res, next) {
  const token = req.cookies.authToken || (req.headers['authorization'] && req.headers['authorization'].split(' ')[1]);
  
  if (!token) {
    return res.status(401).json({ error: 'Access token required' });
  }
  
  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({ error: 'Invalid or expired token' });
    }
    req.user = user;
    next();
  });
}

// Register route
app.post('/register', async (req, res) => {
  try {
    const { playerName, playerTag, telegramInitData } = req.body;
    
    // Validate playerName
    if (!playerName || playerName.length > 13 || !/^[a-zA-Z0-9]+$/.test(playerName)) {
      return res.status(400).json({ 
        error: 'Player name must be 1-13 characters, letters and numbers only, no spaces' 
      });
    }
    
    // Validate playerTag
    if (!playerTag || playerTag.length > 10 || /\s/.test(playerTag)) {
      return res.status(400).json({ 
        error: 'Player tag must be 1-10 characters with no spaces' 
      });
    }
    
    // Validate Telegram initData
    const telegramUser = validateTelegramInit(telegramInitData);
    if (!telegramUser) {
      return res.status(400).json({ error: 'Invalid Telegram initData' });
    }
    
    const telegramId = telegramUser.id;
    
    // Check if user already exists
    const existingUser = await usersCollection.findOne({ telegramId });
    if (existingUser) {
      return res.status(400).json({ error: 'User already registered' });
    }
    
    // Generate unique userId
    let userId;
    let isUnique = false;
    while (!isUnique) {
      userId = generateUserId();
      const existing = await usersCollection.findOne({ userId });
      if (!existing) {
        isUnique = true;
      }
    }
    
    // Create user document
    const user = {
      userId,
      telegramId,
      playerName,
      playerTag,
      coins: 0,
      tokens: 0,
      level: 1,
      premium: false,
      points: 0,
      wins: 0,
      losses: 0,
      gamesPlayed: 0,
      dateJoined: new Date(),
      rankedStats: {
        normalclassic: { rank: 'bronze', position: 0, mythicRank: 0, wins: 0, losses: 0 },
        hardclassic: { rank: 'bronze', position: 0, mythicRank: 0, wins: 0, losses: 0 },
        normalrnd: { rank: 'bronze', position: 0, mythicRank: 0, wins: 0, losses: 0 },
        hardrnd: { rank: 'bronze', position: 0, mythicRank: 0, wins: 0, losses: 0 }
      },
      seasonHistory: [], // Array of { season, normalclassic: {rank, mythicRank}, hardclassic: {...}, ... }
      highestRanks: {
        normalclassic: { rank: 'bronze', mythicRank: 0, season: 0 },
        hardclassic: { rank: 'bronze', mythicRank: 0, season: 0 },
        normalrnd: { rank: 'bronze', mythicRank: 0, season: 0 },
        hardrnd: { rank: 'bronze', mythicRank: 0, season: 0 }
      },
      currentSeason: 1
    };
    
    await usersCollection.insertOne(user);
    
    // Generate JWT (7 days)
    const token = jwt.sign(
      { userId, telegramId },
      JWT_SECRET,
      { expiresIn: '7d' }
    );
    
    // Set HTTP-only cookie
    res.cookie('authToken', token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'none',
      maxAge: 7 * 24 * 60 * 60 * 1000 // 7 days
    });
    
    res.status(201).json({
      message: 'User registered successfully',
      user: {
        userId,
        playerName,
        playerTag,
        level: user.level
      }
    });
    
  } catch (error) {
    console.error('Registration error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Login route
app.post('/login', async (req, res) => {
  try {
    const { telegramInitData } = req.body;
    
    // Validate Telegram initData
    const telegramUser = validateTelegramInit(telegramInitData);
    if (!telegramUser) {
      return res.status(400).json({ error: 'Invalid Telegram initData' });
    }
    
    const telegramId = telegramUser.id;
    
    // Find user
    const user = await usersCollection.findOne({ telegramId });
    if (!user) {
      return res.status(404).json({ error: 'User not found. Please register first.' });
    }
    
    // Generate JWT (7 days)
    const token = jwt.sign(
      { userId: user.userId, telegramId },
      JWT_SECRET,
      { expiresIn: '7d' }
    );
    
    // Set HTTP-only cookie
    res.cookie('authToken', token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'none',
      maxAge: 7 * 24 * 60 * 60 * 1000 // 7 days
    });
    
    res.json({
      message: 'Login successful',
      user: {
        userId: user.userId,
        playerName: user.playerName,
        playerTag: user.playerTag,
        level: user.level
      }
    });
    
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get user info route
app.get('/user', authenticateToken, async (req, res) => {
  try {
    const user = await usersCollection.findOne(
      { userId: req.user.userId },
      { projection: { _id: 0 } }
    );
    
    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    res.json({ user });
    
  } catch (error) {
    console.error('Get user error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get user profile with season info
app.get('/profile', authenticateToken, async (req, res) => {
  try {
    const user = await usersCollection.findOne(
      { userId: req.user.userId },
      { projection: { _id: 0 } }
    );
    
    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    // Get last season data
    const lastSeasonData = user.seasonHistory && user.seasonHistory.length > 0
      ? user.seasonHistory[user.seasonHistory.length - 1]
      : null;
    
    res.json({
      userId: user.userId,
      playerName: user.playerName,
      playerTag: user.playerTag,
      level: user.level,
      points: user.points,
      premium: user.premium || false,
      currentSeason: user.currentSeason || 1,
      currentSeasonRanks: {
        normalclassic: {
          rank: user.rankedStats.normalclassic.rank,
          position: user.rankedStats.normalclassic.position,
          mythicRank: user.rankedStats.normalclassic.mythicRank,
          wins: user.rankedStats.normalclassic.wins,
          losses: user.rankedStats.normalclassic.losses
        },
        hardclassic: {
          rank: user.rankedStats.hardclassic.rank,
          position: user.rankedStats.hardclassic.position,
          mythicRank: user.rankedStats.hardclassic.mythicRank,
          wins: user.rankedStats.hardclassic.wins,
          losses: user.rankedStats.hardclassic.losses
        },
        normalrnd: {
          rank: user.rankedStats.normalrnd.rank,
          position: user.rankedStats.normalrnd.position,
          mythicRank: user.rankedStats.normalrnd.mythicRank,
          wins: user.rankedStats.normalrnd.wins,
          losses: user.rankedStats.normalrnd.losses
        },
        hardrnd: {
          rank: user.rankedStats.hardrnd.rank,
          position: user.rankedStats.hardrnd.position,
          mythicRank: user.rankedStats.hardrnd.mythicRank,
          wins: user.rankedStats.hardrnd.wins,
          losses: user.rankedStats.hardrnd.losses
        }
      },
      lastSeasonRanks: lastSeasonData ? {
        season: lastSeasonData.season,
        normalclassic: lastSeasonData.normalclassic,
        hardclassic: lastSeasonData.hardclassic,
        normalrnd: lastSeasonData.normalrnd,
        hardrnd: lastSeasonData.hardrnd
      } : null,
      highestRanks: user.highestRanks || {
        normalclassic: { rank: 'bronze', mythicRank: 0, season: 0 },
        hardclassic: { rank: 'bronze', mythicRank: 0, season: 0 },
        normalrnd: { rank: 'bronze', mythicRank: 0, season: 0 },
        hardrnd: { rank: 'bronze', mythicRank: 0, season: 0 }
      },
      stats: {
        gamesPlayed: user.gamesPlayed,
        wins: user.wins,
        losses: user.losses
      }
    });
    
  } catch (error) {
    console.error('Get profile error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Create room route
app.post('/create', authenticateToken, async (req, res) => {
  try {
    const { gameType } = req.body;
    const creatorId = req.user.userId;
    
    // Validate game type
    const validGameTypes = ['normalclassic', 'hardclassic', 'normalrnd', 'hardrnd'];
    if (!gameType || !validGameTypes.includes(gameType)) {
      return res.status(400).json({ 
        error: 'Invalid game type. Must be: normalclassic, hardclassic, normalrnd, or hardrnd' 
      });
    }
    
    // Check if creator already has an active room
    const existingRoom = await roomsCollection.findOne({ 
      creatorId,
      status: { $in: ['waiting', 'started'] }
    });
    
    if (existingRoom) {
      return res.status(400).json({ 
        error: 'You already have an active room',
        roomId: existingRoom.roomId
      });
    }
    
    // Generate unique room ID
    let roomId;
    let isUnique = false;
    while (!isUnique) {
      roomId = generateRoomId();
      const existing = await roomsCollection.findOne({ roomId });
      if (!existing) {
        isUnique = true;
      }
    }
    
    // Get creator info
    const creator = await usersCollection.findOne({ userId: creatorId });
    
    // Create room
    const room = {
      roomId,
      gameType,
      creatorId,
      status: 'waiting',
      players: [{
        userId: creatorId,
        playerName: creator.playerName,
        playerTag: creator.playerTag,
        premium: creator.premium || false,
        isCreator: true
      }],
      createdAt: new Date()
    };
    
    await roomsCollection.insertOne(room);
    
    res.status(201).json({
      message: 'Room created successfully',
      room: {
        roomId,
        gameType,
        status: room.status,
        players: room.players
      }
    });
    
  } catch (error) {
    console.error('Create room error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Join room route
app.post('/join', authenticateToken, async (req, res) => {
  try {
    const { roomId } = req.body;
    const userId = req.user.userId;
    
    if (!roomId) {
      return res.status(400).json({ error: 'Room ID is required' });
    }
    
    // Find room
    const room = await roomsCollection.findOne({ roomId: parseInt(roomId) });
    
    if (!room) {
      return res.status(404).json({ error: 'Room not found' });
    }
    
    if (room.status !== 'waiting') {
      return res.status(400).json({ error: 'Room is not accepting players' });
    }
    
    // Check max players (15)
    if (room.players.length >= 15) {
      return res.status(400).json({ error: 'Room is full (maximum 15 players)' });
    }
    
    // Check if user already in room
    const alreadyInRoom = room.players.some(p => p.userId === userId);
    if (alreadyInRoom) {
      return res.status(400).json({ error: 'You are already in this room' });
    }
    
    // Get user info
    const user = await usersCollection.findOne({ userId });
    
    // Add player to room atomically (prevents race condition)
    const result = await roomsCollection.updateOne(
      { 
        roomId: parseInt(roomId),
        status: 'waiting',
        'players.userId': { $ne: userId }, // Ensure user not already in array
        'players.14': { $exists: false } // Ensure room has less than 15 players
      },
      { 
        $push: { 
          players: {
            userId,
            playerName: user.playerName,
            playerTag: user.playerTag,
            premium: user.premium || false,
            isCreator: false
          }
        }
      }
    );
    
    // Check if update succeeded
    if (result.matchedCount === 0) {
      return res.status(400).json({ error: 'Failed to join room. Room may be full or you are already in it.' });
    }
    
    const updatedRoom = await roomsCollection.findOne({ roomId: parseInt(roomId) });
    
    // Broadcast room update to all players in waiting room
    io.to(`waiting_${parseInt(roomId)}`).emit('room_update', {
      roomId: updatedRoom.roomId,
      gameType: updatedRoom.gameType,
      status: updatedRoom.status,
      creatorId: updatedRoom.creatorId,
      players: updatedRoom.players,
      playerCount: updatedRoom.players.length,
      maxPlayers: 15
    });
    
    res.json({
      message: 'Joined room successfully',
      room: {
        roomId: updatedRoom.roomId,
        gameType: updatedRoom.gameType,
        status: updatedRoom.status,
        players: updatedRoom.players,
        playerCount: updatedRoom.players.length,
        maxPlayers: 15
      }
    });
    
  } catch (error) {
    console.error('Join room error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get room info route
app.get('/room/:roomId', authenticateToken, async (req, res) => {
  try {
    const roomId = parseInt(req.params.roomId);
    
    const room = await roomsCollection.findOne({ roomId }, { projection: { _id: 0 } });
    
    if (!room) {
      return res.status(404).json({ error: 'Room not found' });
    }
    
    res.json({
      room: {
        roomId: room.roomId,
        gameType: room.gameType,
        status: room.status,
        creatorId: room.creatorId,
        players: room.players,
        playerCount: room.players.length,
        maxPlayers: 15,
        createdAt: room.createdAt
      }
    });
    
  } catch (error) {
    console.error('Get room error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Start game route
app.post('/start', authenticateToken, async (req, res) => {
  try {
    const { roomId } = req.body;
    const userId = req.user.userId;
    
    if (!roomId) {
      return res.status(400).json({ error: 'Room ID is required' });
    }
    
    // Find room
    const room = await roomsCollection.findOne({ roomId: parseInt(roomId) });
    
    if (!room) {
      return res.status(404).json({ error: 'Room not found' });
    }
    
    // Check if user is the creator
    if (room.creatorId !== userId) {
      return res.status(403).json({ error: 'Only the room creator can start the game' });
    }
    
    // Check room status
    if (room.status !== 'waiting') {
      return res.status(400).json({ error: 'Game has already started or ended' });
    }
    
    // Check minimum players
    if (room.players.length < 2) {
      return res.status(400).json({ error: 'Not enough players. Minimum 2 players required.' });
    }
    
    // Start the game (atomic operation to prevent double-start)
    const result = await roomsCollection.updateOne(
      { roomId: parseInt(roomId), status: 'waiting', creatorId: userId },
      { 
        $set: { 
          status: 'started',
          startedAt: new Date()
        }
      }
    );
    
    if (result.modifiedCount === 0) {
      return res.status(400).json({ error: 'Game already started or you are not the creator' });
    }
    
    const updatedRoom = await roomsCollection.findOne({ roomId: parseInt(roomId) });
    
    // Notify waiting room that game is starting
    io.to(`waiting_${parseInt(roomId)}`).emit('game_starting', {
      message: 'Game is starting! Connect to the game now.',
      roomId: updatedRoom.roomId
    });
    
    res.json({
      message: 'Game started successfully',
      room: {
        roomId: updatedRoom.roomId,
        gameType: updatedRoom.gameType,
        status: updatedRoom.status,
        players: updatedRoom.players,
        startedAt: updatedRoom.startedAt
      }
    });
    
  } catch (error) {
    console.error('Start game error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get game info route
app.get('/game/:roomId', authenticateToken, async (req, res) => {
  try {
    const roomId = parseInt(req.params.roomId);
    
    const game = await gamesCollection.findOne({ roomId }, { projection: { _id: 0 } });
    
    if (!game) {
      return res.status(404).json({ error: 'Game not found' });
    }
    
    res.json({ game });
    
  } catch (error) {
    console.error('Get game error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Matchmaking route
app.post('/matchmake', authenticateToken, async (req, res) => {
  try {
    const { mode, playerCount } = req.body;
    const userId = req.user.userId;
    
    // Validate mode
    const validModes = ['normalclassic', 'hardclassic', 'normalrnd', 'hardrnd'];
    if (!mode || !validModes.includes(mode)) {
      return res.status(400).json({ error: 'Invalid mode' });
    }
    
    // Validate player count
    if (playerCount !== 2 && playerCount !== 4) {
      return res.status(400).json({ error: 'Player count must be 2 (1v1) or 4' });
    }
    
    // Check if user is already in queue
    const alreadyInQueue = await matchmakingCollection.findOne({ userId });
    if (alreadyInQueue) {
      return res.status(400).json({ error: 'You are already in matchmaking queue' });
    }
    
    // Get user info
    const user = await usersCollection.findOne({ userId });
    const modeKey = mode;
    const queueKey = `${mode}_${playerCount === 2 ? '1v1' : '4player'}`;
    
    // Add to matchmaking collection with timestamp
    await matchmakingCollection.insertOne({
      userId,
      playerName: user.playerName,
      playerTag: user.playerTag,
      premium: user.premium || false,
      mode: queueKey,
      rank: user.rankedStats[modeKey].rank,
      mythicRank: user.rankedStats[modeKey].mythicRank || 0,
      createdAt: new Date()
    });
    
    // Try to find match
    setTimeout(() => tryMatchmaking(queueKey, playerCount), 100);
    
    res.json({
      message: 'Joined matchmaking queue',
      mode: queueKey,
      estimatedWaitTime: '0-120 seconds'
    });
    
  } catch (error) {
    console.error('Matchmaking error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Cancel matchmaking route
app.post('/matchmake/cancel', authenticateToken, async (req, res) => {
  try {
    const userId = req.user.userId;
    
    const result = await matchmakingCollection.deleteOne({ userId });
    
    if (result.deletedCount === 0) {
      return res.status(400).json({ error: 'Not in matchmaking queue' });
    }
    
    res.json({ message: 'Matchmaking cancelled' });
    
  } catch (error) {
    console.error('Cancel matchmaking error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Leaderboard route
app.get('/leaderboard/:mode', authenticateToken, async (req, res) => {
  try {
    const mode = req.params.mode;
    const userId = req.user.userId;
    
    // Validate mode
    const validModes = ['normalclassic', 'hardclassic', 'normalrnd', 'hardrnd'];
    if (!validModes.includes(mode)) {
      return res.status(400).json({ error: 'Invalid mode' });
    }
    
    // Get mythic players first
    const mythicPlayers = await usersCollection.find({
      [`rankedStats.${mode}.rank`]: 'mythic'
    })
      .sort({ [`rankedStats.${mode}.mythicRank`]: -1 })
      .limit(100)
      .project({
        userId: 1,
        playerName: 1,
        playerTag: 1,
        level: 1,
        premium: 1,
        [`rankedStats.${mode}`]: 1,
        _id: 0
      })
      .toArray();
    
    let top100 = mythicPlayers;
    
    // If less than 100 mythic players, fill with other ranks
    if (mythicPlayers.length < 100) {
      const remaining = 100 - mythicPlayers.length;
      
      // Get players from other ranks, sorted by rank tier
      const rankOrder = ['legend', 'diamond', 'gold', 'silver', 'bronze'];
      const otherPlayers = [];
      
      for (const rank of rankOrder) {
        if (otherPlayers.length >= remaining) break;
        
        const playersInRank = await usersCollection.find({
          [`rankedStats.${mode}.rank`]: rank
        })
          .sort({ [`rankedStats.${mode}.wins`]: -1 }) // Sort by wins within rank
          .limit(remaining - otherPlayers.length)
          .project({
            userId: 1,
            playerName: 1,
            playerTag: 1,
            level: 1,
            premium: 1,
            [`rankedStats.${mode}`]: 1,
            _id: 0
          })
          .toArray();
        
        otherPlayers.push(...playersInRank);
      }
      
      top100 = [...mythicPlayers, ...otherPlayers];
    }
    
    // Get user's position
    const userRank = await usersCollection.findOne({ userId });
    let userPosition = null;
    
    if (userRank.rankedStats[mode].rank === 'mythic') {
      const higherPlayers = await usersCollection.countDocuments({
        [`rankedStats.${mode}.mythicRank`]: { $gt: userRank.rankedStats[mode].mythicRank }
      });
      userPosition = higherPlayers + 1;
    } else {
      // For non-mythic players, calculate position based on all players above them
      const rankOrderValue = {
        'mythic': 5,
        'legend': 4,
        'diamond': 3,
        'gold': 2,
        'silver': 1,
        'bronze': 0
      };
      
      const userRankValue = rankOrderValue[userRank.rankedStats[mode].rank];
      
      // Count all mythic players
      let higherPlayers = await usersCollection.countDocuments({
        [`rankedStats.${mode}.rank`]: 'mythic'
      });
      
      // Count players in higher rank tiers
      for (const [rank, value] of Object.entries(rankOrderValue)) {
        if (value > userRankValue) {
          const count = await usersCollection.countDocuments({
            [`rankedStats.${mode}.rank`]: rank
          });
          higherPlayers += count;
        }
      }
      
      // Count players in same rank with more wins
      const sameRankHigher = await usersCollection.countDocuments({
        [`rankedStats.${mode}.rank`]: userRank.rankedStats[mode].rank,
        [`rankedStats.${mode}.wins`]: { $gt: userRank.rankedStats[mode].wins }
      });
      
      higherPlayers += sameRankHigher;
      userPosition = higherPlayers + 1;
    }
    
    res.json({
      mode,
      top100: top100.map((player, index) => ({
        position: index + 1,
        userId: player.userId,
        playerName: player.playerName,
        playerTag: player.playerTag,
        level: player.level,
        premium: player.premium || false,
        rank: player.rankedStats[mode].rank,
        rankPosition: player.rankedStats[mode].position,
        mythicRank: player.rankedStats[mode].mythicRank,
        wins: player.rankedStats[mode].wins,
        losses: player.rankedStats[mode].losses
      })),
      userPosition: userPosition ? {
        position: userPosition,
        rank: userRank.rankedStats[mode].rank,
        rankPosition: userRank.rankedStats[mode].position,
        mythicRank: userRank.rankedStats[mode].mythicRank,
        wins: userRank.rankedStats[mode].wins,
        losses: userRank.rankedStats[mode].losses,
        premium: userRank.premium || false
      } : null
    });
    
  } catch (error) {
    console.error('Leaderboard error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Telegram Bot - Admin command for new season
const ADMIN_TELEGRAM_ID = parseInt(process.env.ADMIN_TELEGRAM_ID || '0'); // Set this in .env

// Helper: Generate user challenges
async function generateUserChallenges(userId, type = 'daily') {
  const challenges = type === 'daily' ? DAILY_CHALLENGES : WEEKLY_CHALLENGES;
  const expiresAt = new Date();
  
  if (type === 'daily') {
    expiresAt.setHours(23, 59, 59, 999); // End of day
  } else {
    expiresAt.setDate(expiresAt.getDate() + 7); // 7 days from now
  }
  
  // Randomly select 3 challenges
  const selectedChallenges = challenges
    .sort(() => Math.random() - 0.5)
    .slice(0, 3)
    .map(challenge => ({
      ...challenge,
      progress: 0,
      completed: false
    }));
  
  await challengesCollection.insertOne({
    userId,
    type,
    challenges: selectedChallenges,
    createdAt: new Date(),
    expiresAt
  });
  
  return selectedChallenges;
}

// Helper: Update challenge progress
async function updateChallengeProgress(userId, challengeId, increment = 1) {
  const userChallenges = await challengesCollection.findOne({ userId, 'challenges.id': challengeId });
  
  if (!userChallenges) return null;
  
  const challengeIndex = userChallenges.challenges.findIndex(c => c.id === challengeId);
  const challenge = userChallenges.challenges[challengeIndex];
  
  if (challenge.completed) return null;
  
  const newProgress = challenge.progress + increment;
  const isCompleted = newProgress >= challenge.requirement;
  
  await challengesCollection.updateOne(
    { userId, 'challenges.id': challengeId },
    {
      $set: {
        [`challenges.${challengeIndex}.progress`]: newProgress,
        [`challenges.${challengeIndex}.completed`]: isCompleted
      }
    }
  );
  
  // Award rewards if completed
  if (isCompleted) {
    await usersCollection.updateOne(
      { userId },
      {
        $inc: {
          coins: challenge.reward.coins,
          points: challenge.reward.points
        }
      }
    );
    
    return challenge.reward;
  }
  
  return null;
}

// Get user challenges route
app.get('/challenges', authenticateToken, async (req, res) => {
  try {
    const userId = req.user.userId;
    
    let dailyChallenges = await challengesCollection.findOne({ userId, type: 'daily' });
    let weeklyChallenges = await challengesCollection.findOne({ userId, type: 'weekly' });
    
    // Generate new challenges if none exist or expired
    if (!dailyChallenges) {
      const challenges = await generateUserChallenges(userId, 'daily');
      dailyChallenges = { challenges, type: 'daily', expiresAt: new Date() };
    }
    
    if (!weeklyChallenges) {
      const challenges = await generateUserChallenges(userId, 'weekly');
      weeklyChallenges = { challenges, type: 'weekly', expiresAt: new Date() };
    }
    
    res.json({
      daily: {
        challenges: dailyChallenges.challenges,
        expiresAt: dailyChallenges.expiresAt
      },
      weekly: {
        challenges: weeklyChallenges.challenges,
        expiresAt: weeklyChallenges.expiresAt
      }
    });
    
  } catch (error) {
    console.error('Get challenges error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Bot command to add premium status (admin only)
bot.command('addpremium', async (ctx) => {
  try {
    const adminId = ctx.from.id;
    
    // Check if user is admin
    if (adminId !== ADMIN_TELEGRAM_ID) {
      return ctx.reply('❌ Unauthorized. This command is only available to admins.');
    }
    
    // Parse command: /addpremium <userId>
    const args = ctx.message.text.split(' ');
    if (args.length < 2) {
      return ctx.reply('❌ Usage: /addpremium <userId>\nExample: /addpremium 123456789');
    }
    
    const targetUserId = parseInt(args[1]);
    
    if (isNaN(targetUserId)) {
      return ctx.reply('❌ Invalid user ID. Please provide a numeric user ID.');
    }
    
    // Find user
    const user = await usersCollection.findOne({ userId: targetUserId });
    
    if (!user) {
      return ctx.reply(`❌ User not found with ID: ${targetUserId}`);
    }
    
    // Check if already premium
    if (user.premium) {
      return ctx.reply(`ℹ️ User ${user.playerName} (${targetUserId}) is already premium.`);
    }
    
    // Grant premium
    await usersCollection.updateOne(
      { userId: targetUserId },
      { $set: { premium: true } }
    );
    
    await ctx.reply(
      `✅ Premium granted!\n\n` +
      `👤 Player: ${user.playerName}\n` +
      `🆔 User ID: ${targetUserId}\n` +
      `⭐ Premium: Activated`
    );
    
    console.log(`[ADMIN] Premium granted to user ${targetUserId} by admin ${adminId}`);
    
  } catch (error) {
    console.error('Add premium error:', error);
    await ctx.reply('❌ Error granting premium. Check server logs.');
  }
});

// Bot command to remove premium status (admin only)
bot.command('removepremium', async (ctx) => {
  try {
    const adminId = ctx.from.id;
    
    // Check if user is admin
    if (adminId !== ADMIN_TELEGRAM_ID) {
      return ctx.reply('❌ Unauthorized. This command is only available to admins.');
    }
    
    // Parse command: /removepremium <userId>
    const args = ctx.message.text.split(' ');
    if (args.length < 2) {
      return ctx.reply('❌ Usage: /removepremium <userId>\nExample: /removepremium 123456789');
    }
    
    const targetUserId = parseInt(args[1]);
    
    if (isNaN(targetUserId)) {
      return ctx.reply('❌ Invalid user ID. Please provide a numeric user ID.');
    }
    
    // Find user
    const user = await usersCollection.findOne({ userId: targetUserId });
    
    if (!user) {
      return ctx.reply(`❌ User not found with ID: ${targetUserId}`);
    }
    
    // Check if not premium
    if (!user.premium) {
      return ctx.reply(`ℹ️ User ${user.playerName} (${targetUserId}) is not premium.`);
    }
    
    // Remove premium
    await usersCollection.updateOne(
      { userId: targetUserId },
      { $set: { premium: false } }
    );
    
    await ctx.reply(
      `✅ Premium removed!\n\n` +
      `👤 Player: ${user.playerName}\n` +
      `🆔 User ID: ${targetUserId}\n` +
      `⭐ Premium: Deactivated`
    );
    
    console.log(`[ADMIN] Premium removed from user ${targetUserId} by admin ${adminId}`);
    
  } catch (error) {
    console.error('Remove premium error:', error);
    await ctx.reply('❌ Error removing premium. Check server logs.');
  }
});

bot.command('newseason', async (ctx) => {
  try {
    const userId = ctx.from.id;
    
    // Check if user is admin
    if (userId !== ADMIN_TELEGRAM_ID) {
      return ctx.reply('❌ Unauthorized. This command is only available to admins.');
    }
    
    await ctx.reply('🔄 Starting new season... This may take a moment.');
    
    // Get current season number
    const sampleUser = await usersCollection.findOne({});
    const currentSeason = sampleUser?.currentSeason || 1;
    const newSeason = currentSeason + 1;
    
    console.log(`[ADMIN] Season reset initiated by Telegram user ${userId}`);
    console.log(`[ADMIN] Current season: ${currentSeason}, New season: ${newSeason}`);
    
    // Get all users to save their season history
    const allUsers = await usersCollection.find({}).toArray();
    
    for (const user of allUsers) {
      // Save current season to history
      const seasonEntry = {
        season: currentSeason,
        normalclassic: { ...user.rankedStats.normalclassic },
        hardclassic: { ...user.rankedStats.hardclassic },
        normalrnd: { ...user.rankedStats.normalrnd },
        hardrnd: { ...user.rankedStats.hardrnd }
      };
      
      // Determine new rank based on current rank
      const updates = { currentSeason: newSeason };
      
      for (const mode of ['normalclassic', 'hardclassic', 'normalrnd', 'hardrnd']) {
        const currentRank = user.rankedStats[mode].rank;
        
        if (currentRank === 'mythic') {
          // Mythic → Diamond
          updates[`rankedStats.${mode}.rank`] = 'diamond';
          updates[`rankedStats.${mode}.position`] = 0;
          updates[`rankedStats.${mode}.mythicRank`] = 0;
          updates[`rankedStats.${mode}.wins`] = RANK_SYSTEM.diamond.minWins;
        } else if (currentRank === 'legend') {
          // Legend → Gold
          updates[`rankedStats.${mode}.rank`] = 'gold';
          updates[`rankedStats.${mode}.position`] = 0;
          updates[`rankedStats.${mode}.wins`] = RANK_SYSTEM.gold.minWins;
        }
        // Other ranks stay the same
      }
      
      await usersCollection.updateOne(
        { userId: user.userId },
        {
          $push: { seasonHistory: seasonEntry },
          $set: updates
        }
      );
    }
    
    console.log('[ADMIN] Season reset completed successfully');
    
    await ctx.reply(`✅ Season ${newSeason} started successfully!\n\n` +
      `📊 Reset stats:\n` +
      `• Mythic → Diamond\n` +
      `• Legend → Gold\n` +
      `• Season history saved for all players\n\n` +
      `Total players affected: ${allUsers.length}`);
    
  } catch (error) {
    console.error('New season error:', error);
    await ctx.reply('❌ Error starting new season. Check server logs.');
  }
});

bot.command('season', async (ctx) => {
  try {
    const user = await usersCollection.findOne({ telegramId: ctx.from.id });
    
    if (!user) {
      return ctx.reply('❌ You need to register first! Use /start to register.');
    }
    
    const currentSeason = user.currentSeason || 1;
    const lastSeason = user.seasonHistory?.length > 0 
      ? user.seasonHistory[user.seasonHistory.length - 1] 
      : null;
    
    let message = `📅 Season ${currentSeason}\n\n`;
    message += `🏆 Current Ranks:\n`;
    message += `• Normal Classic: ${user.rankedStats.normalclassic.rank.toUpperCase()}`;
    if (user.rankedStats.normalclassic.rank === 'mythic') {
      message += ` ${user.rankedStats.normalclassic.mythicRank}`;
    } else {
      message += ` ${user.rankedStats.normalclassic.position}`;
    }
    message += `\n`;
    
    if (lastSeason) {
      message += `\n📜 Last Season (${lastSeason.season}):\n`;
      message += `• Normal Classic: ${lastSeason.normalclassic.rank.toUpperCase()}`;
      if (lastSeason.normalclassic.rank === 'mythic') {
        message += ` ${lastSeason.normalclassic.mythicRank}`;
      }
      message += `\n`;
    }
    
    message += `\n⭐ Highest Ranks (All Time):\n`;
    message += `• Normal Classic: ${user.highestRanks.normalclassic.rank.toUpperCase()}`;
    if (user.highestRanks.normalclassic.rank === 'mythic') {
      message += ` ${user.highestRanks.normalclassic.mythicRank}`;
    }
    message += ` (Season ${user.highestRanks.normalclassic.season || currentSeason})`;
    
    await ctx.reply(message);
    
  } catch (error) {
    console.error('Season command error:', error);
    await ctx.reply('❌ Error fetching season info.');
  }
});

// Matchmaking logic
async function tryMatchmaking(queueKey, playerCount) {
  try {
    const playersInQueue = await matchmakingCollection.find({ mode: queueKey })
      .sort({ createdAt: 1 })
      .toArray();
    
    if (playersInQueue.length < playerCount) {
      return; // Not enough players
    }
    
    // Smart matchmaking: prioritize same rank
    const grouped = {};
    playersInQueue.forEach(player => {
      const key = player.rank === 'mythic' 
        ? `mythic_${Math.floor(player.mythicRank / 50)}` // Group mythic players by 50-rank chunks
        : player.rank;
      
      if (!grouped[key]) grouped[key] = [];
      grouped[key].push(player);
    });
    
    // Try to match within same rank group
    for (const group of Object.values(grouped)) {
      if (group.length >= playerCount) {
        await createRankedMatch(group.slice(0, playerCount), queueKey);
        return;
      }
    }
    
    // Expand: match across ranks if players have been waiting >30 seconds
    const waitingLong = playersInQueue.filter(p => 
      (new Date() - new Date(p.createdAt)) > 30000
    );
    
    if (waitingLong.length >= playerCount) {
      await createRankedMatch(waitingLong.slice(0, playerCount), queueKey);
      return;
    }
    
    // If still not matched after 2 minutes, auto-cancel (handled by TTL index)
    
  } catch (error) {
    console.error('Matchmaking logic error:', error);
  }
}

// Create ranked match
async function createRankedMatch(players, queueKey) {
  try {
    const userIds = players.map(p => p.userId);
    
    // Atomic deletion to prevent double-matching
    // Use deleteMany with $in to atomically remove all matched players
    const deleteResult = await matchmakingCollection.deleteMany({
      userId: { $in: userIds },
      mode: queueKey
    });
    
    // If we didn't delete exactly the expected number, some players were already matched
    if (deleteResult.deletedCount !== players.length) {
      console.warn(`Match creation race detected: expected ${players.length}, deleted ${deleteResult.deletedCount}`);
      return; // Abort this match attempt
    }
    
    // Generate room ID
    let roomId;
    let isUnique = false;
    while (!isUnique) {
      roomId = generateRoomId();
      const existing = await roomsCollection.findOne({ roomId });
      if (!existing) {
        isUnique = true;
      }
    }
    
    // Extract game type from queue key
    const gameType = queueKey.replace('_1v1', '').replace('_4player', '');
    
    // Create room
    const room = {
      roomId,
      gameType,
      creatorId: players[0].userId,
      status: 'started',
      isRanked: true,
      players: players.map(p => ({
        userId: p.userId,
        playerName: p.playerName,
        playerTag: p.playerTag,
        premium: p.premium || false,
        isCreator: false
      })),
      createdAt: new Date(),
      startedAt: new Date()
    };
    
    await roomsCollection.insertOne(room);
    
    // Notify players via WebSocket
    players.forEach(player => {
      io.to(`user_${player.userId}`).emit('match_found', {
        roomId,
        gameType,
        players: room.players
      });
    });
    
  } catch (error) {
    console.error('Create ranked match error:', error);
  }
}

// WebSocket Authentication Middleware
io.use((socket, next) => {
  const token = socket.handshake.auth.token;
  
  if (!token) {
    return next(new Error('Authentication error'));
  }
  
  jwt.verify(token, JWT_SECRET, (err, decoded) => {
    if (err) {
      return next(new Error('Authentication error'));
    }
    socket.userId = decoded.userId;
    socket.telegramId = decoded.telegramId;
    next();
  });
});

// WebSocket Connection Handler
io.on('connection', async (socket) => {
  console.log(`User connected: ${socket.userId}`);
  
  // Join user-specific room for notifications
  socket.join(`user_${socket.userId}`);
  
  // Join game room
  socket.on('join_game', async ({ roomId }) => {
    try {
      const parsedRoomId = parseInt(roomId);
      
      // Verify room exists and is started
      const room = await roomsCollection.findOne({ roomId: parsedRoomId });
      
      if (!room) {
        socket.emit('error', { message: 'Room not found' });
        return;
      }
      
      if (room.status !== 'started') {
        socket.emit('error', { message: 'Game has not started yet' });
        return;
      }
      
      // Check if user is in the room
      const isInRoom = room.players.some(p => p.userId === socket.userId);
      
      if (!isInRoom) {
        socket.emit('error', { message: 'You are not in this room' });
        return;
      }
      
      socket.join(`game_${parsedRoomId}`);
      socket.roomId = parsedRoomId;
      
      // Initialize game if not already initialized
      if (!activeGames.has(parsedRoomId)) {
        await initializeGame(parsedRoomId, room);
      }
      
      const gameState = activeGames.get(parsedRoomId);
      
      // Send game state to the joined player
      socket.emit('game_state', {
        gameType: gameState.gameType,
        minLetters: gameState.minLetters,
        currentPlayerIndex: gameState.currentPlayerIndex,
        currentPlayer: gameState.activePlayers[gameState.currentPlayerIndex],
        nextPlayer: gameState.activePlayers[(gameState.currentPlayerIndex + 1) % gameState.activePlayers.length],
        activePlayers: gameState.activePlayers,
        eliminatedPlayers: gameState.eliminatedPlayers,
        spectators: gameState.spectators,
        currentLetter: gameState.currentLetter,
        timeLimit: gameState.timeLimit,
        roundsPassed: gameState.roundsPassed,
        turnHistory: gameState.turnHistory,
        status: gameState.status
      });
      
    } catch (error) {
      console.error('Join game error:', error);
      socket.emit('error', { message: 'Failed to join game' });
    }
  });
  
  // Spectate game (for eliminated players)
  socket.on('spectate_game', async ({ roomId }) => {
    try {
      const parsedRoomId = parseInt(roomId);
      
      const gameState = activeGames.get(parsedRoomId);
      
      if (!gameState) {
        socket.emit('error', { message: 'Game not found' });
        return;
      }
      
      // Check if player was eliminated from this game
      const isSpectator = gameState.spectators.find(s => s.userId === socket.userId);
      
      if (!isSpectator) {
        socket.emit('error', { message: 'You cannot spectate this game' });
        return;
      }
      
      socket.join(`game_${parsedRoomId}`);
      socket.roomId = parsedRoomId;
      
      // Send spectator view
      socket.emit('spectator_state', {
        gameType: gameState.gameType,
        minLetters: gameState.minLetters,
        currentPlayer: gameState.activePlayers[gameState.currentPlayerIndex],
        nextPlayer: gameState.activePlayers[(gameState.currentPlayerIndex + 1) % gameState.activePlayers.length],
        activePlayers: gameState.activePlayers,
        turnHistory: gameState.turnHistory,
        status: gameState.status,
        isSpectating: true
      });
      
    } catch (error) {
      console.error('Spectate game error:', error);
      socket.emit('error', { message: 'Failed to spectate game' });
    }
  });
  
  // Join waiting room (for waiting page real-time updates)
  socket.on('join_waiting_room', async ({ roomId }) => {
    try {
      const parsedRoomId = parseInt(roomId);
      
      // Verify room exists
      const room = await roomsCollection.findOne({ roomId: parsedRoomId });
      
      if (!room) {
        socket.emit('error', { message: 'Room not found' });
        return;
      }
      
      // Check if user is in the room
      const isInRoom = room.players.some(p => p.userId === socket.userId);
      
      if (!isInRoom) {
        socket.emit('error', { message: 'You are not in this room' });
        return;
      }
      
      socket.join(`waiting_${parsedRoomId}`);
      
      // Send current room state
      socket.emit('room_update', {
        roomId: room.roomId,
        gameType: room.gameType,
        status: room.status,
        creatorId: room.creatorId,
        players: room.players,
        playerCount: room.players.length,
        maxPlayers: 15
      });
      
    } catch (error) {
      console.error('Join waiting room error:', error);
      socket.emit('error', { message: 'Failed to join waiting room' });
    }
  });
  
  // Submit word
  socket.on('submit_word', async ({ word }) => {
    try {
      if (!socket.roomId) {
        socket.emit('error', { message: 'Not in a game' });
        return;
      }
      
      const gameState = activeGames.get(socket.roomId);
      
      if (!gameState) {
        socket.emit('error', { message: 'Game not found' });
        return;
      }
      
      if (gameState.status !== 'playing') {
        socket.emit('error', { message: 'Game is not active' });
        return;
      }
      
      const currentPlayer = gameState.activePlayers[gameState.currentPlayerIndex];
      
      if (currentPlayer.userId !== socket.userId) {
        socket.emit('error', { message: 'Not your turn' });
        return;
      }
      
      // Clear existing timer from registry to prevent double-trigger
      if (gameTimers.has(socket.roomId)) {
        clearTimeout(gameTimers.get(socket.roomId));
        gameTimers.delete(socket.roomId);
      }
      
      const wordUpper = word.toUpperCase();
      
      // Validate word length
      if (word.length < gameState.minLetters) {
        socket.emit('word_rejected', { 
          reason: `Word must be at least ${gameState.minLetters} letters`,
          word 
        });
        // Restart timer after rejection
        startTurn(socket.roomId);
        return;
      }
      
      // Validate starting letter
      if (!wordUpper.startsWith(gameState.currentLetter)) {
        socket.emit('word_rejected', { 
          reason: `Word must start with ${gameState.currentLetter}`,
          word 
        });
        // Restart timer after rejection
        startTurn(socket.roomId);
        return;
      }
      
      // Check if word was already used in this game
      const alreadyUsed = gameState.usedWords.has(wordUpper);
      if (alreadyUsed) {
        socket.emit('word_rejected', { 
          reason: 'Word already used in this game',
          word 
        });
        // Restart timer after rejection
        startTurn(socket.roomId);
        return;
      }
      
      // Verify word with API
      const isValid = await verifyWord(word);
      
      if (!isValid) {
        socket.emit('word_rejected', { 
          reason: 'Invalid word',
          word 
        });
        // Restart timer after rejection
        startTurn(socket.roomId);
        return;
      }
      
      // Word accepted
      gameState.usedWords.add(wordUpper);
      
      // Track turn history
      const turnStartTime = Date.now();
      const turnTime = gameTimers.has(socket.roomId) ? gameState.timeLimit : 0;
      
      gameState.turnHistory.push({
        userId: socket.userId,
        playerName: currentPlayer.playerName,
        word: wordUpper,
        timeTaken: Math.max(0, gameState.timeLimit - (turnTime || 0)),
        timestamp: new Date()
      });
      
      // Reset AFK counter for this player
      gameState.afkTracking.set(socket.userId, 0);
      
      // Update player stats
      if (!gameState.playerStats[socket.userId]) {
        gameState.playerStats[socket.userId] = {
          wordsUsed: [],
          longestWord: ''
        };
      }
      
      gameState.playerStats[socket.userId].wordsUsed.push(wordUpper);
      
      if (wordUpper.length > (gameState.playerStats[socket.userId].longestWord?.length || 0)) {
        gameState.playerStats[socket.userId].longestWord = wordUpper;
      }
      
      // Broadcast word accepted
      io.to(`game_${socket.roomId}`).emit('word_accepted', {
        userId: socket.userId,
        playerName: currentPlayer.playerName,
        word: wordUpper,
        wordLength: wordUpper.length,
        turnHistory: gameState.turnHistory
      });
      
      // Move to next turn
      await nextTurn(socket.roomId);
      
    } catch (error) {
      console.error('Submit word error:', error);
      socket.emit('error', { message: 'Failed to submit word' });
    }
  });
  
  socket.on('disconnect', () => {
    console.log(`User disconnected: ${socket.userId}`);
    
    // If user was in an active game, eliminate them after grace period
    if (socket.roomId) {
      const gameState = activeGames.get(socket.roomId);
      
      if (gameState && gameState.status === 'playing') {
        // Clear any existing disconnect timer
        if (disconnectTimers.has(socket.userId)) {
          clearTimeout(disconnectTimers.get(socket.userId));
        }
        
        // Give 10 second grace period for reconnection
        const timer = setTimeout(() => {
          const stillActive = gameState.activePlayers.find(p => p.userId === socket.userId);
          if (stillActive) {
            eliminatePlayer(socket.roomId, socket.userId, 'disconnect');
          }
          disconnectTimers.delete(socket.userId);
        }, 10000); // 10 second grace
        
        disconnectTimers.set(socket.userId, timer);
      }
    }
  });
  
  // Handle reconnection
  socket.on('reconnect_game', ({ roomId }) => {
    // Clear disconnect timer from registry
    if (disconnectTimers.has(socket.userId)) {
      clearTimeout(disconnectTimers.get(socket.userId));
      disconnectTimers.delete(socket.userId);
    }
    
    // Check if player is still in the game
    const gameState = activeGames.get(parseInt(roomId));
    if (gameState) {
      const isActive = gameState.activePlayers.find(p => p.userId === socket.userId);
      const isEliminated = gameState.eliminatedPlayers.find(p => p.userId === socket.userId);
      
      if (isEliminated) {
        // Inform player they were eliminated
        socket.emit('already_eliminated', {
          message: 'You were eliminated while disconnected',
          reason: isEliminated.reason,
          placement: isEliminated.placement,
          canSpectate: true
        });
      } else if (isActive) {
        // Player is still active, send current game state
        socket.join(`game_${roomId}`);
        socket.roomId = roomId;
        
        socket.emit('game_state', {
          gameType: gameState.gameType,
          minLetters: gameState.minLetters,
          currentPlayerIndex: gameState.currentPlayerIndex,
          currentPlayer: gameState.activePlayers[gameState.currentPlayerIndex],
          activePlayers: gameState.activePlayers,
          eliminatedPlayers: gameState.eliminatedPlayers,
          currentLetter: gameState.currentLetter,
          timeLimit: gameState.timeLimit,
          roundsPassed: gameState.roundsPassed,
          status: gameState.status
        });
      }
    }
  });
});

// Initialize game
async function initializeGame(roomId, room) {
  // Hard limit on active games - fail fast if exceeded
  if (activeGames.size >= MAX_ACTIVE_GAMES) {
    console.error(`Max active games (${MAX_ACTIVE_GAMES}) reached. Rejecting new game.`);
    throw new Error('Server at capacity. Please try again later.');
  }
  
  const gameType = room.gameType;
  const isHardMode = gameType.startsWith('hard');
  const isRandom = gameType.includes('rnd');
  
  // Shuffle players for random turn order
  const shuffledPlayers = [...room.players].sort(() => Math.random() - 0.5);
  
  const gameState = {
    roomId,
    gameType,
    activePlayers: shuffledPlayers,
    eliminatedPlayers: [],
    spectators: [], // Eliminated players who are spectating
    currentPlayerIndex: 0,
    minLetters: isHardMode ? 10 : 3,
    maxLetters: 10,
    currentLetter: getRandomLetter(),
    isRandom,
    isRanked: room.isRanked || false,
    usedWords: new Set(),
    roundsPassed: 0,
    playersPlayedThisRound: new Set(),
    timeLimit: isHardMode ? 20 : 40,
    status: 'playing',
    playerStats: {},
    turnHistory: [], // Track all turns for transparency
    afkTracking: new Map(), // userId -> consecutiveMissedTurns
    createdAt: new Date()
  };
  
  // Initialize player stats
  shuffledPlayers.forEach(player => {
    gameState.playerStats[player.userId] = {
      wordsUsed: [],
      longestWord: '',
      hadTimeout: false
    };
  });
  
  activeGames.set(roomId, gameState);
  
  // Save game to database
  await gamesCollection.insertOne({
    roomId,
    gameType,
    players: shuffledPlayers.map(p => p.userId),
    status: 'playing',
    createdAt: new Date()
  });
  
  // Start first turn
  startTurn(roomId);
}

// Start turn with timer
function startTurn(roomId) {
  const gameState = activeGames.get(roomId);
  
  if (!gameState) return;
  
  const currentPlayer = gameState.activePlayers[gameState.currentPlayerIndex];
  const nextPlayerIndex = (gameState.currentPlayerIndex + 1) % gameState.activePlayers.length;
  const nextPlayer = gameState.activePlayers[nextPlayerIndex];
  
  // Broadcast turn start
  io.to(`game_${roomId}`).emit('turn_start', {
    currentPlayer: {
      userId: currentPlayer.userId,
      playerName: currentPlayer.playerName
    },
    nextPlayer: {
      userId: nextPlayer.userId,
      playerName: nextPlayer.playerName
    },
    currentLetter: gameState.currentLetter,
    minLetters: gameState.minLetters,
    timeLimit: gameState.timeLimit,
    roundsPassed: gameState.roundsPassed,
    turnHistory: gameState.turnHistory
  });
  
  // Clear existing timer from registry
  if (gameTimers.has(roomId)) {
    clearTimeout(gameTimers.get(roomId));
  }
  
  // Set timer for elimination in central registry
  const timer = setTimeout(() => {
    // Increment AFK counter
    const currentAfk = gameState.afkTracking.get(currentPlayer.userId) || 0;
    gameState.afkTracking.set(currentPlayer.userId, currentAfk + 1);
    
    // Eliminate if 2+ consecutive missed turns (AFK detection)
    const eliminationReason = currentAfk >= 1 ? 'afk' : 'timeout';
    
    eliminatePlayer(roomId, currentPlayer.userId, eliminationReason);
    gameTimers.delete(roomId);
  }, gameState.timeLimit * 1000);
  
  gameTimers.set(roomId, timer);
}

// Eliminate player
async function eliminatePlayer(roomId, userId, reason) {
  const gameState = activeGames.get(roomId);
  
  if (!gameState) return;
  
  // Clear timer from registry to prevent double-trigger
  if (gameTimers.has(roomId)) {
    clearTimeout(gameTimers.get(roomId));
    gameTimers.delete(roomId);
  }
  
  const playerIndex = gameState.activePlayers.findIndex(p => p.userId === userId);
  
  if (playerIndex === -1) return;
  
  const eliminatedPlayer = gameState.activePlayers[playerIndex];
  
  // Determine if this is a rage quit (disconnect in ranked game)
  const isRageQuit = reason === 'disconnect' && gameState.isRanked;
  
  // Remove from active players
  gameState.activePlayers.splice(playerIndex, 1);
  
  // Add to eliminated players with placement
  const eliminatedEntry = {
    ...eliminatedPlayer,
    placement: gameState.activePlayers.length + 1,
    reason,
    isRageQuit
  };
  
  gameState.eliminatedPlayers.push(eliminatedEntry);
  
  // Add to spectators
  gameState.spectators.push({
    userId: eliminatedPlayer.userId,
    playerName: eliminatedPlayer.playerName,
    canSpectate: true
  });
  
  // Update player stats
  if (gameState.playerStats[userId]) {
    gameState.playerStats[userId].placement = gameState.activePlayers.length + 1;
    gameState.playerStats[userId].eliminated = true;
    gameState.playerStats[userId].eliminationReason = reason;
    gameState.playerStats[userId].isRageQuit = isRageQuit;
    
    // Mark timeout for challenge tracking
    if (reason === 'timeout' || reason === 'afk') {
      gameState.playerStats[userId].hadTimeout = true;
    }
  }
  
  // Apply immediate rank penalty for rage quit in ranked games
  if (isRageQuit) {
    const mode = gameState.gameType;
    // Double penalty: loss + extra derank
    await updateUserRank(userId, mode, false); // Normal loss
    await updateUserRank(userId, mode, false); // Extra penalty for rage quit
    
    console.log(`[RAGE QUIT] User ${userId} penalized in ${mode}`);
  }
  
  // Broadcast elimination
  io.to(`game_${roomId}`).emit('player_eliminated', {
    userId,
    playerName: eliminatedPlayer.playerName,
    reason,
    isRageQuit,
    remainingPlayers: gameState.activePlayers.length,
    canSpectate: true
  });
  
  // Check if game is over
  if (gameState.activePlayers.length === 1) {
    await endGame(roomId);
    return;
  }
  
  // If current player was eliminated, adjust index
  if (gameState.currentPlayerIndex >= gameState.activePlayers.length) {
    gameState.currentPlayerIndex = 0;
  }
  
  // Continue to next turn
  await nextTurn(roomId);
}

// Next turn
async function nextTurn(roomId) {
  const gameState = activeGames.get(roomId);
  
  if (!gameState || gameState.status !== 'playing') return;
  
  const currentPlayer = gameState.activePlayers[gameState.currentPlayerIndex];
  
  // Mark that this player played this round
  gameState.playersPlayedThisRound.add(currentPlayer.userId);
  
  // Move to next player
  gameState.currentPlayerIndex = (gameState.currentPlayerIndex + 1) % gameState.activePlayers.length;
  
  // Check if all players have played this round
  if (gameState.playersPlayedThisRound.size === gameState.activePlayers.length) {
    gameState.roundsPassed++;
    gameState.playersPlayedThisRound.clear();
    
    // Every 2 rounds, increase difficulty (only in normal mode and if not at max)
    if (!gameState.gameType.startsWith('hard') && gameState.roundsPassed % 2 === 0 && gameState.minLetters < gameState.maxLetters) {
      gameState.minLetters++;
      gameState.timeLimit = getTimeLimit(gameState.minLetters);
      
      io.to(`game_${roomId}`).emit('difficulty_increased', {
        minLetters: gameState.minLetters,
        timeLimit: gameState.timeLimit
      });
    }
  }
  
  // Set next letter
  if (gameState.isRandom) {
    gameState.currentLetter = getRandomLetter();
  } else {
    // Use last letter of the word that was just played (previous player)
    const previousIndex = (gameState.currentPlayerIndex - 1 + gameState.activePlayers.length) % gameState.activePlayers.length;
    const previousPlayer = gameState.activePlayers[previousIndex];
    const previousPlayerStats = gameState.playerStats[previousPlayer.userId];
    
    if (previousPlayerStats?.wordsUsed?.length > 0) {
      const lastWord = previousPlayerStats.wordsUsed[previousPlayerStats.wordsUsed.length - 1];
      gameState.currentLetter = lastWord[lastWord.length - 1];
    } else {
      gameState.currentLetter = getRandomLetter();
    }
  }
  
  // Start next turn
  startTurn(roomId);
}

// End game
async function endGame(roomId) {
  const gameState = activeGames.get(roomId);
  
  if (!gameState) return;
  
  gameState.status = 'finished';
  
  // Clear timer from registry
  if (gameTimers.has(roomId)) {
    clearTimeout(gameTimers.get(roomId));
    gameTimers.delete(roomId);
  }
  
  // Winner is the last remaining player
  const winner = gameState.activePlayers[0];
  
  // Update player stats for winner
  if (gameState.playerStats[winner.userId]) {
    gameState.playerStats[winner.userId].placement = 1;
    gameState.playerStats[winner.userId].isWinner = true;
  }
  
  const totalPlayers = gameState.eliminatedPlayers.length + 1;
  
  // Calculate points and update database
  const allPlayers = [winner, ...gameState.eliminatedPlayers];
  
  for (const player of allPlayers) {
    const stats = gameState.playerStats[player.userId];
    const isWinner = player.userId === winner.userId;
    const points = calculatePoints(stats, isWinner, totalPlayers);
    
    // Update user in database
    const updateData = {
      $inc: {
        points: points,
        wins: isWinner ? 1 : 0,
        losses: isWinner ? 0 : 1,
        gamesPlayed: 1
      }
    };
    
    await usersCollection.updateOne({ userId: player.userId }, updateData);
    
    // Calculate and update level
    const user = await usersCollection.findOne({ userId: player.userId });
    const newLevel = calculateLevel(user.points);
    
    if (newLevel > user.level) {
      await usersCollection.updateOne(
        { userId: player.userId },
        { $set: { level: newLevel } }
      );
    }
    
    // Update ranked stats if this is a ranked game
    if (gameState.isRanked) {
      const mode = gameState.gameType;
      const newRank = await updateUserRank(player.userId, mode, isWinner);
      stats.newRank = newRank;
      stats.rankChange = newRank.rankChange;
      
      // Update challenges
      if (isWinner) {
        await updateChallengeProgress(player.userId, 'win_2_ranked');
        await updateChallengeProgress(player.userId, 'win_10_ranked');
      }
      
      // Check if reached gold or higher
      const rankIndex = RANK_ORDER.indexOf(newRank.rank);
      if (rankIndex >= 2) { // Gold or higher
        await updateChallengeProgress(player.userId, 'reach_gold');
      }
    }
    
    // Track games played
    await updateChallengeProgress(player.userId, 'play_5_games');
    await updateChallengeProgress(player.userId, 'play_30_games');
    
    // Track longest word
    if (stats.longestWord && stats.longestWord.length >= 7) {
      await updateChallengeProgress(player.userId, 'play_7_letter');
    }
    if (stats.longestWord && stats.longestWord.length >= 9) {
      await updateChallengeProgress(player.userId, 'longest_word_9');
    }
    
    // Track perfect game (no timeouts)
    if (isWinner && !stats.hadTimeout) {
      await updateChallengeProgress(player.userId, 'win_no_timeout');
    }
    
    stats.pointsEarned = points;
  }
  
  // Runner up (second place)
  const runnerUp = gameState.eliminatedPlayers.find(p => p.placement === 2);
  
  // Calculate post-game summary stats
  const fastestTurn = gameState.turnHistory.reduce((fastest, turn) => {
    return (!fastest || turn.timeTaken < fastest.timeTaken) ? turn : fastest;
  }, null);
  
  const longestWordOverall = gameState.turnHistory.reduce((longest, turn) => {
    return (!longest || turn.word.length > longest.word.length) ? turn : longest;
  }, null);
  
  // Game results
  const results = {
    winner: {
      userId: winner.userId,
      playerName: winner.playerName,
      playerTag: winner.playerTag,
      premium: winner.premium || false,
      points: gameState.playerStats[winner.userId].pointsEarned,
      longestWord: gameState.playerStats[winner.userId].longestWord,
      wordsUsed: gameState.playerStats[winner.userId].wordsUsed.length,
      newRank: gameState.playerStats[winner.userId].newRank,
      rankChange: gameState.playerStats[winner.userId].rankChange
    },
    runnerUp: runnerUp ? {
      userId: runnerUp.userId,
      playerName: runnerUp.playerName,
      playerTag: runnerUp.playerTag,
      premium: runnerUp.premium || false,
      points: gameState.playerStats[runnerUp.userId].pointsEarned,
      longestWord: gameState.playerStats[runnerUp.userId].longestWord,
      newRank: gameState.playerStats[runnerUp.userId].newRank,
      rankChange: gameState.playerStats[runnerUp.userId].rankChange
    } : null,
    eliminationOrder: gameState.eliminatedPlayers.map(p => ({
      userId: p.userId,
      playerName: p.playerName,
      playerTag: p.playerTag,
      premium: p.premium || false,
      placement: p.placement,
      reason: p.reason,
      isRageQuit: p.isRageQuit,
      newRank: gameState.playerStats[p.userId].newRank,
      rankChange: gameState.playerStats[p.userId].rankChange
    })),
    gameStats: {
      totalWords: gameState.usedWords.size,
      finalDifficulty: gameState.minLetters,
      gameType: gameState.gameType,
      isRanked: gameState.isRanked,
      gameDuration: Math.floor((new Date() - gameState.createdAt) / 1000), // seconds
      turnHistory: gameState.turnHistory
    },
    postGameSummary: {
      longestWordOverall: longestWordOverall ? {
        word: longestWordOverall.word,
        playerName: longestWordOverall.playerName,
        length: longestWordOverall.word.length
      } : null,
      fastestTurn: fastestTurn ? {
        word: fastestTurn.word,
        playerName: fastestTurn.playerName,
        timeTaken: fastestTurn.timeTaken
      } : null,
      totalTurns: gameState.turnHistory.length,
      averageTurnTime: gameState.turnHistory.length > 0 
        ? Math.floor(gameState.turnHistory.reduce((sum, t) => sum + t.timeTaken, 0) / gameState.turnHistory.length)
        : 0
    }
  };
  
  // Save final game state to database
  await gamesCollection.updateOne(
    { roomId },
    {
      $set: {
        status: 'finished',
        results,
        finishedAt: new Date()
      }
    }
  );
  
  // Update room status
  await roomsCollection.updateOne(
    { roomId },
    { $set: { status: 'finished' } }
  );
  
  // Broadcast game end
  io.to(`game_${roomId}`).emit('game_ended', results);
  
  // Clean up active game immediately to prevent memory buildup
  // Store finishedAt timestamp before deletion for periodic cleanup fallback
  gameState.finishedAt = new Date();
  
  // Delete after 1 minute to allow late joiners to see results
  setTimeout(() => {
    activeGames.delete(roomId);
    console.log(`Cleaned up game: ${roomId}`);
  }, 60 * 1000);
}

// Start server
httpServer.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

// Periodic memory cleanup (every 10 minutes)
setInterval(() => {
  const now = Date.now();
  
  // Clean up finished games older than 5 minutes
  for (const [roomId, gameState] of activeGames.entries()) {
    if (gameState.status === 'finished') {
      const finishedTime = gameState.finishedAt?.getTime() || gameState.createdAt.getTime();
      if (now - finishedTime > 5 * 60 * 1000) {
        activeGames.delete(roomId);
        gameTimers.delete(roomId); // Clean up timer registry
        console.log(`Cleaned up finished game: ${roomId}`);
      }
    }
    
    // Clean up stale playersPlayedThisRound Sets (safety net)
    if (gameState.playersPlayedThisRound && gameState.playersPlayedThisRound.size > 0) {
      const gameAge = now - gameState.createdAt.getTime();
      if (gameAge > 30 * 60 * 1000) { // Game older than 30 mins with stale data
        console.warn(`Cleaning stale playersPlayedThisRound for game ${roomId}`);
        gameState.playersPlayedThisRound.clear();
      }
    }
  }
  
  // Clean up orphaned disconnect timers
  for (const [userId, timer] of disconnectTimers.entries()) {
    // If timer is very old, clear it
    clearTimeout(timer);
    disconnectTimers.delete(userId);
  }
  
  // Log memory usage
  const memUsage = process.memoryUsage();
  console.log('Memory cleanup:', {
    activeGames: activeGames.size,
    gameTimers: gameTimers.size,
    disconnectTimers: disconnectTimers.size,
    cachedWords: verifiedWordsCache.size,
    heapUsed: `${Math.round(memUsage.heapUsed / 1024 / 1024)}MB`
  });
}, 10 * 60 * 1000); // Every 10 minutes

// Bot launch
bot.launch().then(() => {
  console.log('Telegram bot launched');
});

// Graceful shutdown
process.once('SIGINT', () => {
  bot.stop('SIGINT');
  process.exit(0);
});

process.once('SIGTERM', () => {
  bot.stop('SIGTERM');
  process.exit(0);
});
