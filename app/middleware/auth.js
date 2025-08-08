import jwt from 'jsonwebtoken';

const SUPABASE_JWT_SECRET = process.env.SUPABASE_JWT_SECRET;

export function authenticate(req, res, next) {
    const authHeader = req.headers.authorization;

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(401).json({ error: 'Missing or invalid authorization header' });
    }

    const token = authHeader.split(' ')[1];

    try {
        const decoded = jwt.verify(token, SUPABASE_JWT_SECRET);

        if (!decoded.sub) {
            return res.status(401).json({ error: 'Token missing user ID (sub)' });
        }

        req.user = {
            id: decoded.sub,
            ...decoded,
        };

        next();
    } catch (err) {
        console.error('JWT verification failed:', err);
        return res.status(401).json({ error: 'Invalid or expired token' });
    }
}
